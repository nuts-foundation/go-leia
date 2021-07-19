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

type IndexOption interface{}

type TransformerOption struct {
	IndexOption
	Transformer Transform
}

type TokenizerOption struct {
	IndexOption
	Tokenizer Tokenizer
}

type AliasOption struct {
	IndexOption
	Alias string
}

// NewFieldIndexer creates a new fieldIndexer
// leave the name empty to use the json path as name.
// the name is to be used as query key when searching
func NewFieldIndexer(jsonPath string, options ...IndexOption) FieldIndexer {
	return fieldIndexer{
		path:    jsonPath,
		options: options,
	}
}

type fieldIndexer struct {
	path    string
	options []IndexOption
}

func (j fieldIndexer) getAlias() *string {
	for _, o := range j.options {
		if a, b := o.(AliasOption); b {
			return &a.Alias
		}
	}
	return nil
}

func (j fieldIndexer) getTokenizer() Tokenizer {
	for _, o := range j.options {
		if a, b := o.(TokenizerOption); b {
			return a.Tokenizer
		}
	}
	return nil
}

func (j fieldIndexer) getTransformer() Transform {
	for _, o := range j.options {
		if a, b := o.(TransformerOption); b {
			return a.Transformer
		}
	}
	return nil
}

func (j fieldIndexer) Name() string {
	alias := j.getAlias()
	if alias != nil {
		return *alias
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
	if j.getTokenizer() == nil {
		tokenized = rawKeys
	} else {
		for _, rawKey := range rawKeys {
			tokens := j.Tokenize(rawKey)
			tokenized = append(tokenized, tokens...)
		}
	}

	// run the transformer
	transformed := make([]interface{}, len(tokenized))
	if j.getTransformer() == nil {
		transformed = tokenized
	} else {
		for i, rawKey := range rawKeys {
			transformed[i] = j.getTransformer()(rawKey)
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
	if j.getTokenizer() == nil {
		return []interface{}{value}
	}

	if s, ok := value.(string); ok {
		tokens := j.getTokenizer()(s)
		result := make([]interface{}, len(tokens))
		for i, t := range tokens {
			result[i] = t
		}
		return result
	}
	return []interface{}{value}
}

func (j fieldIndexer) Transform(value interface{}) interface{} {
	if j.getTransformer() == nil {
		return value
	}
	return j.getTransformer()(value)
}
