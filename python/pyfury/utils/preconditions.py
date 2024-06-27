# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


# Check utils
class Preconditions:
    @staticmethod
    def check_not_null(o, error_message=None):
        if o is None:
            if error_message is None:
                raise ValueError("None value provided")
            else:
                raise ValueError(error_message)
        return o

    @staticmethod
    def check_state(expression, error_message=None):
        if not expression:
            if error_message is None:
                raise RuntimeError("Invalid state")
            else:
                raise RuntimeError(error_message)

    @staticmethod
    def check_argument(
        b,
        error_message=None,
        error_message_template=None,
        error_message_arg0=None,
        *error_message_args
    ):
        if not b:
            if error_message_template is not None:
                if error_message_args is not None:
                    args = (error_message_arg0,) + error_message_args
                else:
                    args = (error_message_arg0,)
                raise ValueError(error_message_template % args)
            elif error_message is not None:
                raise ValueError(error_message)
            else:
                raise ValueError("Invalid argument")
