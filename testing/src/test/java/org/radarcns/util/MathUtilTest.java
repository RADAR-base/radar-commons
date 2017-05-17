/*
 * Copyright 2017 The Hyve and King's College London
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarcns.util;

import static org.junit.Assert.*;

import org.junit.Test;

public class MathUtilTest {
    @Test
    public void gcd() throws Exception {
        assertEquals(2L, MathUtil.gcd(4L, 6L));
    }

    @Test
    public void lcm() throws Exception {
        assertEquals(15L, MathUtil.lcm(3L, 5L));
        assertEquals(12L, MathUtil.lcm(4L, 6L));
    }
}