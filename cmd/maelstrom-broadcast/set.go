package main

func setMinus[T comparable](x, y map[T]struct{}) map[T]struct{} {
	res := make(map[T]struct{}, len(x))
	for k := range x {
		if _, ok := y[k]; !ok {
			res[k] = struct{}{}
		}
	}

	return res
}

func setUnion[T comparable](x, y map[T]struct{}) map[T]struct{} {
	res := make(map[T]struct{}, len(x)+len(y))
	for k := range x {
		res[k] = struct{}{}
	}

	for k := range y {
		res[k] = struct{}{}
	}
	return res
}

func setUnion3[T comparable](x, y, z map[T]struct{}) map[T]struct{} {
	res := make(map[T]struct{}, len(x)+len(y)+len(z))
	for k := range x {
		res[k] = struct{}{}
	}

	for k := range y {
		res[k] = struct{}{}
	}

	for k := range z {
		res[k] = struct{}{}
	}

	return res
}

func set2List[T comparable](m map[T]struct{}) []T {
	l := make([]T, 0, len(m))
	for k := range m {
		l = append(l, k)
	}

	return l
}

func list2Set[T comparable](l []T) map[T]struct{} {
	m := make(map[T]struct{}, len(l))
	for _, msg := range l {
		m[msg] = struct{}{}
	}

	return m
}
