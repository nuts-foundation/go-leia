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

// IndexOption is the option function for adding options to a FieldIndexer
type IndexOption func(fieldIndexer *fieldIndexer)

// TransformerOption is the option for a FieldIndexer to apply transformation before indexing the value.
// The transformation is also applied to a query value that matches the indexed field.
func TransformerOption(transformer Transform) IndexOption {
	return func(fieldIndexer *fieldIndexer) {
		fieldIndexer.transformer = transformer
	}
}

// TokenizerOption is the option for a FieldIndexer to split a value to be indexed into multiple parts.
// Each part is then indexed separately.
func TokenizerOption(tokenizer Tokenizer) IndexOption {
	return func(fieldIndexer *fieldIndexer) {
		fieldIndexer.tokenizer = tokenizer
	}
}

// AliasOption is the option for a FieldIndexer to add a custom JSON path that will also resolve to the same Index part
func AliasOption(alias string) IndexOption {
	return func(fieldIndexer *fieldIndexer) {
		fieldIndexer.alias = &alias
	}
}

// NewFieldIndexer creates a new fieldIndexer
// leave the name empty to use the json path as name.
// the name is to be used as query key when searching
func NewFieldIndexer(jsonPath string, options ...IndexOption) FieldIndexer {
	fi := fieldIndexer{
		path: jsonPath,
	}
	for _, o := range options {
		o(&fi)
	}
	return fi
}

type fieldIndexer struct {
	alias       *string
	path        string
	transformer Transform
	tokenizer   Tokenizer
}

func (j fieldIndexer) Name() string {
	if j.alias != nil {
		return *j.alias
	}
	return j.path
}

func (j fieldIndexer) Keys(document Document) ([]Key, error) {
	// first get the raw values from the query path
	rawKeys, err := document.ValuesAtPath(j.path)
	if err != nil {
		return nil, err
	}

	// run the tokenizer
	tokenized := make([]interface{}, 0)
	if j.tokenizer == nil {
		tokenized = rawKeys
	} else {
		for _, rawKey := range rawKeys {
			tokens := j.Tokenize(rawKey)
			tokenized = append(tokenized, tokens...)
		}
	}

	// run the transformer
	transformed := make([]interface{}, len(tokenized))
	if j.transformer == nil {
		transformed = tokenized
	} else {
		for i, rawKey := range tokenized {
			transformed[i] = j.transformer(rawKey)
		}
	}

	// to Keys
	keys := make([]Key, len(transformed))
	for i, t := range transformed {
		keys[i], err = toBytes(t)
		if err != nil {
			return nil, err
		}
	}

	return keys, nil
}

func (j fieldIndexer) Tokenize(value interface{}) []interface{} {
	if j.tokenizer == nil {
		return []interface{}{value}
	}

	if s, ok := value.(string); ok {
		tokens := j.tokenizer(s)
		result := make([]interface{}, len(tokens))
		for i, t := range tokens {
			result[i] = t
		}
		return result
	}
	return []interface{}{value}
}

func (j fieldIndexer) Transform(value interface{}) interface{} {
	if j.transformer == nil {
		return value
	}
	return j.transformer(value)
}
