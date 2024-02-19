package storage

/*
 * inter sorted data
 */

//sorter for `removedBaseFile` by blocks asc
type removedBaseFileSort []*removedBaseFile

func (f removedBaseFileSort) Len() int {
	return len(f)
}
func (f removedBaseFileSort) Less(i, j int) bool {
	return (f)[i].blocks < (f)[j].blocks
}
func (f removedBaseFileSort) Swap(i, j int) {
	(f)[i], (f)[j] = (f)[j], (f)[i]
}