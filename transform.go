/*
 * go-leia
 * Copyright (C) 2021 Nuts community
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package leia

import (
	"regexp"
	"strings"
)

// Transform is a function definition for transforming values and search terms.
type Transform func(string) string

// NoTransform is the default Transform function and returns the given data as is.
func NoTransform(terms string) string {
	return terms
}

// ToLower transforms all Unicode letters mapped to their lower case.
// byte values that do not correspond to letters are ignored.
func ToLower(terms string) string {
	return strings.ToLower(terms)
}

// Tokenizer is a function definition that transforms a text into tokens
type Tokenizer func(string) []string

const nonWhitespaceRegex = "/\\S/gm"

// WhiteSpaceTokenizer tokenizes the string based on the /\S/gm regex
func WhiteSpaceTokenizer(text string) []string {
	exp, _ := regexp.Compile(nonWhitespaceRegex)
	return exp.FindAllString(text, -1)
}

// NoTokenizer returns the given text as single token.
func NoTokenizer(text string) []string {
	return []string{text}
}
