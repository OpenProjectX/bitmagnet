package tracker

import (
	"bufio"
	"errors"
	"io"
	"strconv"
)

// decoder is intentionally bounded and supports incremental dictionary entries.
// Scrape catalogues must never be unmarshalled into one giant map.
type decoder struct{ r *bufio.Reader }

func (d decoder) str() (string, error) {
	var prefix []byte

	for {
		b, err := d.r.ReadByte()
		if err != nil {
			return "", err
		}

		if b == ':' {
			break
		}

		if b < '0' || b > '9' || len(prefix) >= 8 {
			return "", errors.New("invalid bencode string length")
		}

		prefix = append(prefix, b)
	}

	n, err := strconv.Atoi(string(prefix))
	if err != nil || n > 4096 {
		return "", errors.New("bencode string exceeds limit")
	}

	b := make([]byte, n)
	_, err = io.ReadFull(d.r, b)

	return string(b), err
}

func (d decoder) value(depth int) (any, error) {
	if depth > 8 {
		return nil, errors.New("bencode nesting exceeds limit")
	}

	p, err := d.r.Peek(1)
	if err != nil {
		return nil, err
	}

	switch p[0] {
	case 'i':
		_, _ = d.r.ReadByte()

		var b []byte

		for {
			c, e := d.r.ReadByte()
			if e != nil {
				return nil, e
			}

			if c == 'e' {
				break
			}

			if len(b) >= 20 {
				return nil, errors.New("integer too long")
			}

			b = append(b, c)
		}

		n, e := strconv.ParseInt(string(b), 10, 64)

		return n, e
	case 'd':
		_, _ = d.r.ReadByte()
		m := map[string]any{}

		for range 256 {
			end, e := d.end()
			if e != nil {
				return nil, e
			}

			if end {
				return m, nil
			}

			k, e := d.str()
			if e != nil {
				return nil, e
			}

			if _, ok := m[k]; ok {
				return nil, errors.New("duplicate dictionary key")
			}

			v, e := d.value(depth + 1)
			if e != nil {
				return nil, e
			}

			m[k] = v
		}

		return nil, errors.New("dictionary exceeds limit")
	case 'l':
		_, _ = d.r.ReadByte()

		var list []any

		for range 256 {
			end, e := d.end()
			if e != nil {
				return nil, e
			}

			if end {
				return list, nil
			}

			v, e := d.value(depth + 1)
			if e != nil {
				return nil, e
			}

			list = append(list, v)
		}

		return nil, errors.New("list exceeds limit")
	default:
		return d.str()
	}
}

func (d decoder) end() (bool, error) {
	p, e := d.r.Peek(1)
	if e != nil {
		return false, e
	}

	if p[0] == 'e' {
		_, _ = d.r.ReadByte()
		return true, nil
	}

	return false, nil
}

func (d decoder) expect(b byte) error {
	c, e := d.r.ReadByte()
	if e != nil {
		return e
	}

	if c != b {
		return errors.New("unexpected bencode token")
	}

	return nil
}
