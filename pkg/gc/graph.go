package gc

// Graph 是对象引用图，支持环与悬空引用。
type Graph struct {
	byID map[ObjectID]*Resource
	// children 归属边：owner -> 归属于它的对象。
	children map[ObjectID][]ObjectID
	// referrers 反向引用边：被引用者 -> 引用者（含归属）。
	referrers map[ObjectID][]ObjectID
}

// BuildGraph 从快照构建引用图。
func BuildGraph(snap *Snapshot) *Graph {
	g := &Graph{
		byID:      make(map[ObjectID]*Resource, len(snap.Resources)),
		children:  make(map[ObjectID][]ObjectID),
		referrers: make(map[ObjectID][]ObjectID),
	}
	for i := range snap.Resources {
		r := &snap.Resources[i]
		g.byID[r.ID] = r
	}
	for i := range snap.Resources {
		r := &snap.Resources[i]
		if r.Owner != "" {
			g.children[r.Owner] = append(g.children[r.Owner], r.ID)
			g.referrers[r.Owner] = append(g.referrers[r.Owner], r.ID)
		}
		for _, ref := range r.Refs {
			g.referrers[ref] = append(g.referrers[ref], r.ID)
		}
	}
	return g
}

// Get 返回对象，不存在返回 nil。
func (g *Graph) Get(id ObjectID) *Resource { return g.byID[id] }

// Referrers 返回引用了 id 的对象列表（含归属者）。
func (g *Graph) Referrers(id ObjectID) []ObjectID { return g.referrers[id] }

// Reachable 从根集合出发沿正向边（引用 + 归属）遍历，返回所有可达对象。
// 迭代 + visited 集合，环与重复引用不会导致死循环。
func (g *Graph) Reachable(roots []ObjectID) map[ObjectID]bool {
	visited := make(map[ObjectID]bool, len(g.byID))
	stack := append([]ObjectID(nil), roots...)
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if visited[id] {
			continue
		}
		visited[id] = true
		r := g.byID[id]
		if r == nil {
			continue // 根指向不存在的对象，忽略
		}
		stack = append(stack, r.Refs...)
		stack = append(stack, g.children[id]...)
	}
	return visited
}
