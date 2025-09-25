package topk

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type freqs struct {
	keys   []string
	counts map[string]int
}

func (f freqs) Len() int { return len(f.keys) }

// Actually 'Greater', since we want decreasing
func (f *freqs) Less(i, j int) bool {
	return f.counts[f.keys[i]] > f.counts[f.keys[j]] || f.counts[f.keys[i]] == f.counts[f.keys[j]] && f.keys[i] < f.keys[j]
}

func (f *freqs) Swap(i, j int) { f.keys[i], f.keys[j] = f.keys[j], f.keys[i] }

func TestTopK(t *testing.T) {

	f, err := os.Open("testdata/domains.txt")

	if err != nil {
		t.Fatal(err)
	}

	scanner := bufio.NewScanner(f)

	tk := New(100)
	exact := make(map[string]int)
	count := 0

	for scanner.Scan() {

		item := scanner.Text()

		exact[item]++
		count++
		e := tk.Insert(item, 1)
		if e.Count < exact[item] {
			t.Errorf("estimate lower than exact: key=%v, exact=%v, estimate=%v", e.Key, exact[item], e.Count)
		}
		if e.Count-e.Error > exact[item] {
			t.Errorf("error bounds too large: key=%v, count=%v, error=%v, exact=%v", e.Key, e.Count, e.Error, exact[item])
		}
	}

	if err := scanner.Err(); err != nil {
		log.Println("error during scan: ", err)
	}

	assert.Equal(t, count, tk.Count())

	var keys []string

	for k := range exact {
		keys = append(keys, k)
	}

	freq := &freqs{keys: keys, counts: exact}

	sort.Sort(freq)

	top := tk.Keys()

	// at least the top 25 must be in order
	for i := 0; i < 25; i++ {
		if top[i].Key != freq.keys[i] {
			t.Errorf("key mismatch: idx=%d top=%s (%d) exact=%s (%d)", i, top[i].Key, top[i].Count, freq.keys[i], freq.counts[freq.keys[i]])
		}
	}
	for k, v := range exact {
		e := tk.Estimate(k)
		if e.Count < v {
			t.Errorf("estimate lower than exact: key=%v, exact=%v, estimate=%v", e.Key, v, e.Count)
		}
		if e.Count-e.Error > v {
			t.Errorf("error bounds too large: key=%v, count=%v, error=%v, exact=%v", e.Key, e.Count, e.Error, v)
		}
	}
	for _, k := range top {
		e := tk.Estimate(k.Key)
		if e != k {
			t.Errorf("estimate differs from top keys: key=%v, estimate=%v(-%v) top=%v(-%v)", e.Key, e.Count, e.Error, k.Count, k.Error)
		}
	}

	// msgp
	buf := bytes.NewBuffer(nil)
	if err := tk.Encode(buf); err != nil {
		t.Error(err)
	}

	decoded := New(100)
	if err := decoded.Decode(buf); err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(tk, decoded) {
		t.Error("they are not equal.")
	}
}

func TestTopKMerge(t *testing.T) {
	tk1 := New(20)
	tk2 := New(20)
	mtk := New(20)
	count := 0

	for i := 0; i <= 10000; i++ {
		x := rand.ExpFloat64() * 10
		word := fmt.Sprintf("word-%d", int(x))
		tk1.Insert(word, 1)
		mtk.Insert(word, 1)
		count++
	}

	for i := 0; i <= 10000; i++ {
		x := rand.ExpFloat64() * 10
		word := fmt.Sprintf("word-%d", int(x))
		tk2.Insert(word, 1)
		mtk.Insert(word, 1)
		count++
	}

	if err := tk1.Merge(tk2); err != nil {
		t.Error(err)
	}

	r1, r2 := tk1.Keys(), mtk.Keys()
	for i := range r1 {
		fmt.Println(r1[i], r2[i])
		if r1[i] != r2[i] {
			t.Errorf("%v != %v", r1[i], r2[i])
		}
	}
	assert.Equal(t, count, mtk.Count())
}

func loadWords() []string {
	f, _ := os.Open("testdata/words.txt")
	defer f.Close()
	r := bufio.NewReader(f)

	res := make([]string, 0, 1024)
	for i := 0; ; i++ {
		if l, err := r.ReadString('\n'); err != nil {
			if err == io.EOF {
				return res
			}
			panic(err)
		} else {
			l = strings.Trim(l, "\r\n ")
			if len(l) > 0 {
				res = append(res, l)
			}
		}
	}
}

func exactCount(words []string) map[string]int {
	m := make(map[string]int, len(words))
	for _, w := range words {
		if _, ok := m[w]; ok {
			m[w]++
		} else {
			m[w] = 1
		}
	}

	return m
}

func exactTop(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(a, b int) bool {
		return m[keys[a]] > m[keys[b]]
	})

	return keys
}

// epsilon: count should be within exact*epsilon range
// returns: probability that a sample in the sketch lies outside the error range (delta)
func errorRate(epsilon float64, exact map[string]int, sketch map[string]Element) float64 {
	var numOk, numBad int

	for w, wc := range sketch {
		exactwc := float64(exact[w])
		lowerBound := int(math.Floor(exactwc * (1 - epsilon)))
		upperBound := int(math.Ceil(exactwc * (1 + epsilon)))

		if wc.Count-wc.Error < lowerBound || wc.Count-wc.Error > upperBound {
			numBad++
			fmt.Printf("!! %s: %d not in range [%d, %d], epsilon=%f\n", w, wc.Count-wc.Error, lowerBound, upperBound, epsilon)
		} else {
			numOk++
		}
	}

	return float64(numBad) / float64(len(sketch))
}

func resultToMap(result []Element) map[string]Element {
	res := make(map[string]Element, len(result))
	for _, lhh := range result {
		res[lhh.Key] = lhh
	}

	return res
}

func assertErrorRate(t *testing.T, exact map[string]int, result []Element, delta, epsilon float64) {
	t.Helper() // Indicates to the testing framework that this is a helper func to skip in stack traces
	sketch := resultToMap(result)
	effectiveDelta := errorRate(epsilon, exact, sketch)
	if effectiveDelta >= delta {
		t.Errorf("Expected error rate <= %f. Found %f. Sketch size: %d", delta, effectiveDelta, len(sketch))
	}
}

// split and array of strings into n slices
func split(words []string, splits int) [][]string {
	l := len(words)
	step := l / splits

	slices := make([][]string, 0, splits)
	for i := 0; i < splits; i++ {
		if i == splits-1 {
			slices = append(slices, words[i*step:])
		} else {
			slices = append(slices, words[i*step:i*step+step])
		}
	}

	sanityCheck := 0
	for _, slice := range slices {
		sanityCheck += len(slice)
	}
	if sanityCheck != l {
		panic("Internal error")
	}
	if len(slices) != splits {
		panic(fmt.Sprintf("Num splits mismatch %d/%d", len(slices), splits))
	}

	return slices
}

func TestSingle(t *testing.T) {
	delta := 0.05
	topK := 100

	words := loadWords()

	// Words in prime index positions are copied
	for _, p := range []int{2, 3, 5, 7, 11, 13, 17, 23} {
		for i := p; i < len(words); i += p {
			words[i] = words[p]
		}
	}

	sketch := New(topK)

	for _, w := range words {
		sketch.Insert(w, 1)
	}

	exact := exactCount(words)
	top := exactTop(exact)

	assertErrorRate(t, exact, sketch.Keys(), delta, 0.00001)
	//assertErrorRate(t, exact, sketch.Result(1)[:topK], delta, epsilon) // We would LOVE this to pass!

	// Assert order of heavy hitters in sub-sketch is as expected
	// TODO: by way of construction of test set we have pandemonium after #8, would like to check top[:topk]
	skTop := sketch.Keys()
	for i, w := range top[:8] {
		if w != skTop[i].Key && exact[w] != skTop[i].Count {
			fmt.Println("key", w, exact[w])
			t.Errorf("Expected top %d/%d to be '%s'(%d) found '%s'(%d)", i, topK, w, exact[w], skTop[i].Key, skTop[i].Count)
		}
	}
}

func TestTheShebang(t *testing.T) {
	words := loadWords()

	// Words in prime index positions are copied
	for _, p := range []int{2, 3, 5, 7, 11, 13, 17, 23} {
		for i := p; i < len(words); i += p {
			words[i] = words[p]
		}
	}

	cases := []struct {
		name   string
		slices [][]string
		delta  float64
		topk   int
	}{
		{
			name:   "Single slice top20 d=0.01",
			slices: split(words, 1),
			delta:  0.01,
			topk:   20,
		},
		{
			name:   "Two slices top20 d=0.01",
			slices: split(words, 2),
			delta:  0.01,
			topk:   20,
		},
		{
			name:   "Three slices top20 d=0.01",
			slices: split(words, 3),
			delta:  0.01,
			topk:   20,
		},
		{
			name:   "100 slices top20 d=0.01",
			slices: split(words, 100),
			delta:  0.01,
			topk:   20,
		},
	}

	for _, cas := range cases {
		t.Run(cas.name, func(t *testing.T) {
			caseRunner(t, cas.slices, cas.topk, cas.delta)
		})
	}
}

func caseRunner(t *testing.T, slices [][]string, topk int, delta float64) {
	var sketches []*TopK
	var corpusSize int

	// Find corpus size
	for _, slice := range slices {
		corpusSize += len(slice)
	}

	// Build sketches for each slice
	for _, slice := range slices {
		sk := New(topk)
		for _, w := range slice {
			sk.Insert(w, 1)
		}
		exact := exactCount(slice)
		top := exactTop(exact)
		skTop := sk.Keys()

		assertErrorRate(t, exact, skTop, delta, 0.001)

		// Assert order of heavy hitters in sub-sketch is as expected
		// TODO: by way of construction of test set we have pandemonium after #8, would like to check top[:topk]
		for i, w := range top[:8] {
			if w != skTop[i].Key && exact[w] != skTop[i].Count {
				t.Errorf("Expected top %d/%d to be '%s'(%d) found '%s'(%d)", i, topk, w, exact[w], skTop[i].Key, skTop[i].Count)
			}
		}

		sketches = append(sketches, sk)
	}

	if len(slices) == 1 {
		return
	}

	// Compute exact stats for entire corpus
	var allSlice []string
	for _, slice := range slices {
		allSlice = append(allSlice, slice...)
	}
	exactAll := exactCount(allSlice)

	// Merge all sketches
	mainSketch := sketches[0]
	for _, sk := range sketches[1:] {
		mainSketch.Merge(sk)
		// TODO: it would be nice to incrementally check the error rates
	}
	//assertErrorRate(t, exactAll, mainSketch.Keys(), delta, epsilon)

	// Assert order of heavy hitters in final result is as expected
	// TODO: by way of construction of test set we have pandemonium after #8, would like to check top[:topk]
	top := exactTop(exactAll)
	skTop := mainSketch.Keys()
	for i, w := range top[:8] {
		if w != skTop[i].Key {
			t.Errorf("Expected top %d/%d to be '%s'(%d) found '%s'(%d)", i, topk, w, exactAll[w], skTop[i].Key, skTop[i].Count)
		}
	}
}

func TestMarshalUnMarshal(t *testing.T) {
	topK := int(100)

	words := loadWords()

	// Words in prime index positions are copied
	for _, p := range []int{2, 3, 5, 7, 11, 13, 17, 23} {
		for i := p; i < len(words); i += p {
			words[i] = words[p]
		}
	}

	sketch := New(topK)

	for _, w := range words {
		sketch.Insert(w, 1)
	}

	b := bytes.NewBuffer(nil)
	err := sketch.Encode(b)
	assert.NoError(t, err)

	fmt.Println(len(b.Bytes()))

	tmp := &TopK{}
	err = tmp.Decode(b)
	assert.NoError(t, err)
	assert.EqualValues(t, sketch, tmp)

}

func TestTopKClear(t *testing.T) {
	tk := New(10)

	tk.Insert("apple", 5)
	tk.Insert("banana", 3)
	tk.Insert("cherry", 7)
	tk.Insert("date", 2)

	tk.Clear()

	assert.Equal(t, 0, tk.Count())
	keys := tk.Keys()
	assert.Equal(t, 0, len(keys))
	est := tk.Estimate("apple")
	assert.Equal(t, 0, est.Count)
	assert.Equal(t, 10, tk.k)
}

func TestStreamClear(t *testing.T) {
	stream := newStream(10)

	stream.Insert("foo", 5)
	stream.Insert("bar", 3)
	stream.Insert("baz", 8)

	stream.Clear()

	keys := stream.Keys()
	assert.Equal(t, 0, len(keys))

	est := stream.Estimate("foo")
	assert.Equal(t, 0, est.Count)
	assert.Equal(t, 10, stream.n)
}
