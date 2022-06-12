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

#include "exprs/like_predicate.h"

#include <gtest/gtest.h>

#include <string>

#include "common/object_pool.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/runtime_state.h"

#include "testutil/function_utils.h"
#include "testutil/test_util.h"
#include "udf/udf_internal.h"

namespace doris {

// mock
class LikePredicateTest : public testing::Test {
public:
    LikePredicateTest() { }

protected:
    virtual void SetUp() { }
    virtual void TearDown() { }
};

using DataSet = std::vector< std::pair<std::vector<const char*>, uint8_t> >;

TEST_F(LikePredicateTest, like) {
    doris_udf::BooleanVal trueRet = doris_udf::BooleanVal(true);
    doris_udf::BooleanVal falseRet = doris_udf::BooleanVal(false);

    DataSet data_set = {// sub_string
                        {{"abc", "%b%"}, 1},
                        {{"abc", "%ad%"}, 0},
                        // end with
                        {{"abc", "%c"}, 1},
                        {{"ab", "%c"}, 0},
                        // start with
                        {{"abc", "a%"}, 1},
                        {{"bc", "a%"}, 0},
                        // equals
                        {{"abc", "abc"}, 1},
                        {{"abc", "ab"}, 0},
                        // full regexp match
                        {{"abcd", "a_c%"}, 1},
                        {{"abcd", "a_d%"}, 0},
                        {{"abc", "__c"}, 1},
                        {{"abc", "_c"}, 0},
                        {{"abc", "_b_"}, 1},
                        {{"abc", "_a_"}, 0},
                        {{"abc", "a__"}, 1},
                        {{"abc", "a_"}, 0}};

    // constant pattern
    for (auto & data : data_set) {
        std::vector<doris_udf::AnyVal*> constant_args;
        StringVal* str = new StringVal(data.first[0]);
        StringVal* pattern = new StringVal(data.first[1]);
        constant_args.push_back(str);
        constant_args.push_back(pattern);
        FunctionContext * context = new doris_udf::FunctionContext();
        context->impl()->set_constant_args(constant_args);
        LikePredicate::like_prepare(context, FunctionContext::THREAD_LOCAL);
        EXPECT_EQ(data.second ? trueRet : falseRet,
                  LikePredicate::like(context, StringVal(data.first[0]), StringVal("not used"))
                 ) << "data: " << data.first[0] << " pattern: " << data.first[1];
        LikePredicate::like_close(context, FunctionContext::THREAD_LOCAL);
        delete str;
        delete pattern;
        delete context;
    }

    // not constant pattern
    LikePredicate::like_prepare(context, FunctionContext::THREAD_LOCAL);
    for (auto & data : data_set) {
        EXPECT_EQ(data.second ? trueRet : falseRet,
                  LikePredicate::like(context, StringVal(data.first[0]), StringVal(data.first[1]))
                 ) << "data: " << data.first[0] << " pattern: " << data.first[1];
    }
    LikePredicate::like_close(context, FunctionContext::THREAD_LOCAL);
}

TEST_F(LikePredicateTest, regexp) {
    doris_udf::BooleanVal trueRet = doris_udf::BooleanVal(true);
    doris_udf::BooleanVal falseRet = doris_udf::BooleanVal(false);

    DataSet data_set = {// sub_string
                        {{"abc", ".*b.*"}, 1},
                        {{"abc", ".*ad.*"}, 0},
                        {{"abc", ".*c"}, 1},
                        {{"abc", "a.*"}, 1},
                        // end with
                        {{"abc", ".*c$"}, 1},
                        {{"ab", ".*c$"}, 0},
                        // start with
                        {{"abc", "^a.*"}, 1},
                        {{"bc", "^a.*"}, 0},
                        // equals
                        {{"abc", "^abc$"}, 1},
                        {{"abc", "^ab$"}, 0},
                        // partial regexp match
                        {{"abcde", "a.*d"}, 1},
                        {{"abcd", "a.d"}, 0},
                        {{"abc", ".c"}, 1},
                        {{"abc", ".b."}, 1},
                        {{"abc", ".a."}, 0}};

    // constant pattern
    for (auto & data : data_set) {
        std::vector<doris_udf::AnyVal*> constant_args;
        StringVal* str = new StringVal(data.first[0]);
        StringVal* pattern = new StringVal(data.first[1]);
        constant_args.push_back(str);
        constant_args.push_back(pattern);
        FunctionContext * context = new doris_udf::FunctionContext();
        context->impl()->set_constant_args(constant_args);
        LikePredicate::regex_prepare(context, FunctionContext::THREAD_LOCAL);
        EXPECT_EQ(data.second ? trueRet : falseRet,
                  LikePredicate::regex(context, StringVal(data.first[0]), StringVal("not used"))
                 ) << "data: " << data.first[0] << " pattern: " << data.first[1];
        LikePredicate::regex_close(context, FunctionContext::THREAD_LOCAL);
        delete str;
        delete pattern;
        delete context;
    }

    // not constant pattern
    LikePredicate::regex_prepare(context, FunctionContext::THREAD_LOCAL);
    for (auto & data : data_set) {
        EXPECT_EQ(data.second ? trueRet : falseRet,
                  LikePredicate::regex(context, StringVal(data.first[0]), StringVal(data.first[1]))
                 ) << "data: " << data.first[0] << " pattern: " << data.first[1];
    }
    LikePredicate::regex_close(context, FunctionContext::THREAD_LOCAL);
}

} // namespace doris
