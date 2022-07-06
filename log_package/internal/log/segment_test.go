package log

import (
	"io"
	"io/ioutil"
	"log"
	"testing"

	v "github.com/abdulmajid18/log-distributed-system/log_package/api/v1"

	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	path_dir := "/home/rozz/go/src/github.com/abdulmajid18/LogDistributedSystem/log_package/internal/log/"
	dir, err := ioutil.TempDir(path_dir, "segment-test")
	if err != nil {
		log.Fatal(err)
	}
	// f, err := os.Create("segment-test")
	// defer os.RemoveAll(dir)

	want := &v.Record{Value: []byte("hello world")}

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entWidth * 3

	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)
	require.Equal(t, uint64(16), s.nextOffset, s.nextOffset)
	require.False(t, s.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, 16+i, off)

		got, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}

	_, err = s.Append(want)
	require.Equal(t, io.EOF, err)

	// maxed index
	require.True(t, s.IsMaxed())

	c.Segment.MaxStoreBytes = uint64(len(want.Value) * 3)
	c.Segment.MaxIndexBytes = 1024

	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	// maxed store
	require.True(t, s.IsMaxed())

	err = s.Close()
	require.NoError(t, err)
	// err = s.Remove()
	// require.NoError(t, err)
	// s, err = newSegment(dir, 16, c)
	// require.NoError(t, err)
	// require.False(t, s.IsMaxed())
}
