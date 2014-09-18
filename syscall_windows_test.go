package mgo_test

func stop(pid int) (err error) {
	// Should always panic
	panicOnWindows()
	return nil
}

func cont(pid int) (err error) {
	// Should always panic
	panicOnWindows()
	return nil
}
