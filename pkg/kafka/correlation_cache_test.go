// Copyright 2018 Authors of Cilium
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	. "gopkg.in/check.v1"
)

func (k *kafkaTestSuite) TestCorrelation(c *C) {
	cc := NewCorrelationCache()

	request1 := &RequestMessage{rawMsg: []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}}
	response1 := &ResponseMessage{rawMsg: []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}}
	request2 := &RequestMessage{rawMsg: []byte{2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}}
	response2 := &ResponseMessage{rawMsg: []byte{2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}}

	cc.Insert(request1)
	c.Assert(cc.CorrelateResponse(response1), Equals, request1)
	c.Assert(cc.CorrelateResponse(response2), IsNil)

	cc.Insert(request2)
	c.Assert(cc.CorrelateResponse(response1), Equals, request1)
	c.Assert(cc.CorrelateResponse(response2), Equals, request2)

	cc.Delete(request1)
	c.Assert(cc.CorrelateResponse(response1), IsNil)
	c.Assert(cc.CorrelateResponse(response2), Equals, request2)

	cc.Delete(request2)
	c.Assert(cc.CorrelateResponse(response1), IsNil)
	c.Assert(cc.CorrelateResponse(response2), IsNil)

	cc.DeleteCache()
}
