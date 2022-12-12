package mqtt

import (
	"errors"
	"strings"
)

// Subscriptions provides clients and servers with a way to manage requested
// topic published messages. Subscriptions is an abstraction over state, not
// input/output operations, so calls to Subscribe should not write bytes over a transport.
type Subscriptions interface {
	// Subscribe takes a []byte slice to make it explicit and abundantly clear that
	// Subscriptions is in charge of the memory corresponding to subscription topics.
	Subscribe(topic []byte) error

	// Successfully matched topics are stored in the userBuffer and returned
	// as a slice of byte slices.

	// Match finds all subscribers to a topic or a filter.
	Match(topicFilter string, userBuffer []byte) ([][]byte, error)

	Unsubscribe(topicFilter string, userBuffer []byte) ([][]byte, error)
}

// TODO(soypat): Add AVL tree implementation like the one in github.com/soypat/go-canard, supposedly is best data structure for this [citation needed].

var _ Subscriptions = SubscriptionsMap{}

// SubscriptionsMap implements Subscriptions interface with a map.
// It performs allocations.
type SubscriptionsMap map[string]struct{}

func (sm SubscriptionsMap) Subscribe(topic []byte) error {
	tp := string(topic)
	if _, ok := sm[tp]; ok {
		return errors.New("topic already exists in subscriptions")
	}
	sm[tp] = struct{}{}
	return nil
}

func (sm SubscriptionsMap) Unsubscribe(topicFilter string, userBuffer []byte) (matched [][]byte, err error) {
	return sm.match(topicFilter, userBuffer, true)
}

func (sm SubscriptionsMap) Match(topicFilter string, userBuffer []byte) (matched [][]byte, err error) {
	return sm.match(topicFilter, userBuffer, false)
}

func (sm SubscriptionsMap) match(topicFilter string, userBuffer []byte, deleteMatches bool) (matched [][]byte, err error) {
	n := 0 // Bytes copied into userBuffer.
	filterParts := strings.Split(topicFilter, "/")
	if err := validateWildcards(filterParts); err != nil {
		return nil, err
	}

	_, hasNonWildSub := sm[topicFilter]
	if hasNonWildSub {
		if len(topicFilter) > len(userBuffer) {
			return nil, ErrUserBufferFull
		}
		n += copy(userBuffer, topicFilter)
		matched = append(matched, userBuffer[:n])
		userBuffer = userBuffer[n:]
		if deleteMatches {
			delete(sm, topicFilter)
		}
	}

	for k := range sm {
		parts := strings.Split(k, "/")
		if matches(filterParts, parts) {
			if len(k) > len(userBuffer) {
				return matched, ErrUserBufferFull
			}
			n += copy(userBuffer, k)
			matched = append(matched, userBuffer[:n])
			userBuffer = userBuffer[n:]
			if deleteMatches {
				delete(sm, k)
			}
		}
	}
	return matched, nil
}

func matches(filter, topicParts []string) bool {
	i := 0
	for i < len(topicParts) {
		// topic is longer, no match
		if i >= len(filter) {
			return false
		}
		// matched up to here, and now the wildcard says "all others will match"
		if filter[i] == "#" {
			return true
		}
		// text does not match, and there wasn't a + to excuse it
		if topicParts[i] != filter[i] && filter[i] != "+" {
			return false
		}
		i++
	}

	// make finance/stock/ibm/# match finance/stock/ibm
	return i == len(filter)-1 && filter[len(filter)-1] == "#" || i == len(filter)
}

func isWildcard(topic string) bool {
	return strings.IndexByte(topic, '#') >= 0 || strings.IndexByte(topic, '+') >= 0
}

func validateWildcards(wildcards []string) error {
	for i, part := range wildcards {
		// catch things like finance#
		if isWildcard(part) && len(part) != 1 {
			return errors.New("malformed wildcard of style \"finance#\"")
		}
		isSingle := len(part) == 1 && part[0] == '#'
		// # can only occur as the last part
		if isSingle && i != len(wildcards)-1 {
			return errors.New("last wildcard is single \"#\"")
		}
	}
	return nil
}
