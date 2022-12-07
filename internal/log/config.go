package log

type Config struct {
	// Raft struct {
	// 	raft.Config
	// 	StreamLayer *StreamLayer
	// 	Bootstrap
	// 	bool
	// }
	Segment struct {
		MaxStoreBytes uint64
		MaxIndexBytes uint64
		InitialOffset uint64
	}
}
