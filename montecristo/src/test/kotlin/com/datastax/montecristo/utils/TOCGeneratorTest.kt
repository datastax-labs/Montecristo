/*
 * Copyright DataStax, Inc.
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

package com.datastax.montecristo.utils

import org.junit.Test
import kotlin.test.assertEquals

class TOCGeneratorTest {
    val toc = TOCGenerator("""# Heading
            |## subheading
            |random test
            |### another heading
            |
        """.trimMargin())

    @Test
    fun testLinkExtraction() {


        val headers = toc.extractHeaders()
        assertEquals(3, headers.size)
    }

    @Test
    fun testMarkdownGeneration() {
        val markdown = toc.getMarkdown()
        println(markdown)
    }

    @Test
    fun testRemoveCodeBlocks() {
        val textWithComments = """```php
                |<?php echo 'test' ?>
                |```
                | 
                |test
                |
                |```js
                |var item = 0;
                |```
            """.trimMargin()

        assertEquals("""
                |
                | 
                |test
                |
                |
            """.trimMargin(),
                TOCGenerator.removeCodeBlocks(textWithComments))
    }
}