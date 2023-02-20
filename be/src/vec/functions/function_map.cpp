// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_map.h"
#include "vec/data_types/get_least_supertype.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/array/function_array_index.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

// construct a map
// map(key1, value2, key2, value2) -> {key1: value2, key2: value2}
class FunctionMap : public IFunction {
public:
    static constexpr auto name = "map";
    static FunctionPtr create() { return std::make_shared<FunctionMap>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }

    bool use_default_implementation_for_nulls() const override { return false; }

    size_t get_number_of_arguments() const override { return 0; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(arguments.size() % 2 == 0)
                << "function: " << get_name() << ", arguments should not be even number";

        DataTypes key_types;
        DataTypes val_types;
        for (size_t i = 0; i < arguments.size(); i += 2) {
            key_types.push_back(arguments[i]);
            val_types.push_back(arguments[i + 1]);
        }

        DataTypePtr key_type;
        DataTypePtr val_type;
        get_least_supertype(key_types, &key_type);
        get_least_supertype(val_types, &val_type);
        
        return std::make_shared<DataTypeMap>(key_type, val_type);
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        DCHECK(arguments.size() % 2 == 0)
                << "function: " << get_name() << ", arguments should not be even number";

        size_t num_element = arguments.size();

        auto result_col = block.get_by_position(result).type->create_column();
        // auto result_col_map = static_cast<ColumnMap*>(result_col.get());
        // auto col_map = typeid_cast<ColumnMap*>(result_col.get());
        auto col_map = static_cast<ColumnMap*>(result_col.get());
        auto col_map_keys = static_cast<ColumnArray*>(&col_map->get_keys());
        auto col_map_vals = static_cast<ColumnArray*>(&col_map->get_values());

        // map keys array data and offsets
        auto& result_col_map_keys_data = col_map_keys->get_data();
        auto& result_col_map_keys_offsets = col_map_keys->get_offsets();
        result_col_map_keys_data.reserve(input_rows_count * num_element / 2);
        result_col_map_keys_offsets.resize(input_rows_count);
        // map values array data and offsets
        auto& result_col_map_vals_data = col_map_vals->get_data();
        auto& result_col_map_vals_offsets = col_map_vals->get_offsets();
        result_col_map_vals_data.reserve(input_rows_count * num_element / 2);
        result_col_map_vals_offsets.resize(input_rows_count);

        // convert to nullable column
        for (size_t i = 0; i < num_element; ++i) {
            auto& col = block.get_by_position(arguments[i]).column;
            col = col->convert_to_full_column_if_const();
            bool is_nullable = i % 2 == 0 ? 
                result_col_map_keys_data.is_nullable() : result_col_map_vals_data.is_nullable();
            if (is_nullable && !col->is_nullable()) {
                col = ColumnNullable::create(col, ColumnUInt8::create(col->size(), 0));
            }
        }

        // insert value into array
        ColumnArray::Offset64 offset = 0;
        for (size_t row = 0; row < input_rows_count; ++row) {
            for (size_t i = 0; i < num_element; i += 2) {
                result_col_map_keys_data.insert_from(*block.get_by_position(arguments[i]).column, row);
                result_col_map_vals_data.insert_from(*block.get_by_position(arguments[i + 1]).column, row);
            }
            offset += num_element / 2;
            result_col_map_keys_offsets[row] = offset;
            result_col_map_vals_offsets[row] = offset;
        }
        block.replace_by_position(result, std::move(result_col));
        return Status::OK();
    }
};

class FunctionMapSize : public IFunction {
public:
    static constexpr auto name = "map_size";
    static FunctionPtr create() { return std::make_shared<FunctionMapSize>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(is_map(arguments[0]))
                << "first argument for function: " << name << " should be DataTypeMap";
        return std::make_shared<DataTypeInt64>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        auto left_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto col_map = check_and_get_column<ColumnMap>(*left_column);
        if (!col_map) {
            return Status::RuntimeError("unsupported types for function {}({})", get_name(),
                                        block.get_by_position(arguments[0]).type->get_name());
        }

        auto dst_column = ColumnInt64::create(input_rows_count);
        auto& dst_data = dst_column->get_data();

        for (size_t i = 0; i < col_map->size(); i++) {
            dst_data[i] = col_map->size_at(i);
        }

        block.replace_by_position(result, std::move(dst_column));
        return Status::OK();
    }
};

template<bool is_key>
class FunctionMapContains : public IFunction {
public:
    static constexpr auto name = is_key ? "map_contains_key" : "map_contains_value";
    static FunctionPtr create() { return std::make_shared<FunctionMapContains>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(is_map(arguments[0]))
                << "first argument for function: " << name << " should be DataTypeMap";
        
        if (arguments[0]->is_nullable()) {
            return make_nullable(std::make_shared<DataTypeNumber<UInt8>>());
        } else {
            return std::make_shared<DataTypeNumber<UInt8>>();
        }
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        // backup original argument 0
        auto orig_arg0 = block.get_by_position(arguments[0]);
        auto left_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto col_map = check_and_get_column<ColumnMap>(*left_column);
        if (!col_map) {
            return Status::RuntimeError("unsupported types for function {}({})", get_name(),
                                        block.get_by_position(arguments[0]).type->get_name());
        }

        const auto datatype_map = 
            static_cast<const DataTypeMap*>(block.get_by_position(arguments[0]).type.get());
        if constexpr(is_key) {
            block.get_by_position(arguments[0]) = {
                col_map->get_keys_ptr(),
                std::make_shared<DataTypeArray>(datatype_map->get_key_type()),
                block.get_by_position(arguments[0]).name + ".keys"
            };
        } else {
            block.get_by_position(arguments[0]) = {
                col_map->get_values_ptr(),
                std::make_shared<DataTypeArray>(datatype_map->get_value_type()),
                block.get_by_position(arguments[0]).name + ".values"
            };
        }

        RETURN_IF_ERROR(
            array_contains.execute_impl(context, block, arguments, result, input_rows_count));
        
        // restore original argument 0
        block.get_by_position(arguments[0]) = orig_arg0;
        return Status::OK();
    }

private:
    FunctionArrayIndex<ArrayContainsAction, FunctionMapContains<is_key>> array_contains;
};

// template<bool is_key>
// class FunctionMapContainsLike : public IFunction {
// public:
//     static constexpr auto name = is_key ? "map_contains_key_like" : "map_contains_value_like";
//     static FunctionPtr create() { return std::make_shared<FunctionMapContainsLike>(); }

//     /// Get function name.
//     String get_name() const override { return name; }

//     bool is_variadic() const override { return false; }

//     size_t get_number_of_arguments() const override { return 2; }

//     DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
//         DCHECK(is_map(arguments[0]))
//                 << "first argument for function: " << name << " should be DataTypeMap";
        
//         if (arguments[0]->is_nullable()) {
//             return make_nullable(std::make_shared<DataTypeNumber<UInt8>>());
//         } else {
//             return std::make_shared<DataTypeNumber<UInt8>>();
//         }
//     }

//     Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
//                         size_t result, size_t input_rows_count) override {
//         // backup original argument 0
//         auto orig_arg0 = block.get_by_position(arguments[0]);
//         auto left_column =
//                 block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
//         const auto col_map = check_and_get_column<ColumnMap>(*left_column);
//         if (!col_map) {
//             return Status::RuntimeError("unsupported types for function {}({})", get_name(),
//                                         block.get_by_position(arguments[0]).type->get_name());
//         }

//         const auto datatype_map = 
//             static_cast<const DataTypeMap*>(block.get_by_position(arguments[0]).type.get());
//         if constexpr(is_key) {
//             block.get_by_position(arguments[0]) = {
//                 col_map->get_keys_ptr(),
//                 std::make_shared<DataTypeArray>(datatype_map->get_key_type()),
//                 block.get_by_position(arguments[0]).name + ".keys"
//             };
//         } else {
//             block.get_by_position(arguments[0]) = {
//                 col_map->get_values_ptr(),
//                 std::make_shared<DataTypeArray>(datatype_map->get_value_type()),
//                 block.get_by_position(arguments[0]).name + ".values"
//             };
//         }

//         RETURN_IF_ERROR(
//             array_contains.execute_impl(context, block, arguments, result, input_rows_count));
        
//         // restore original argument 0
//         block.get_by_position(arguments[0]) = orig_arg0;
//         return Status::OK();
//     }
// };

template<bool is_key>
class FunctionMapEntries : public IFunction {
public:
    static constexpr auto name = is_key ? "map_keys" : "map_values";
    static FunctionPtr create() { return std::make_shared<FunctionMapEntries>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(is_map(arguments[0]))
                << "first argument for function: " << name << " should be DataTypeMap";
        const auto datatype_map = 
            static_cast<const DataTypeMap*>(arguments[0].get());
        if (is_key) {
            return std::make_shared<DataTypeArray>(datatype_map->get_key_type());
        } else {
            return std::make_shared<DataTypeArray>(datatype_map->get_value_type());
        }
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        auto left_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto col_map = check_and_get_column<ColumnMap>(*left_column);
        if (!col_map) {
            return Status::RuntimeError("unsupported types for function {}({})", get_name(),
                                        block.get_by_position(arguments[0]).type->get_name());
        }

        if constexpr(is_key) {
            block.replace_by_position(result, col_map->get_keys_ptr());
        } else {
            block.replace_by_position(result, col_map->get_values_ptr());
        }

        return Status::OK();
    }
};

void register_function_map(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionMap>();
    factory.register_function<FunctionMapSize>();
    factory.register_function<FunctionMapContains<true>>();
    factory.register_function<FunctionMapContains<false>>();
    factory.register_function<FunctionMapEntries<true>>();
    factory.register_function<FunctionMapEntries<false>>();
}

} // namespace doris::vectorized
