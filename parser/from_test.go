package parser

import (
	"database/sql/driver"
	"github.com/aschoerk/go-sql-mem/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func dv(v ...data.Tuple) *TableViewData {
	res := make([]data.Tuple, len(v))
	for i := range v {
		res[i] = v[i]
	}
	return &TableViewData{cols: []int{0, 1, 2, 3, 4}[:v[0].DataLen()], tuples: res}
}

func v(a ...driver.Value) data.Tuple {
	res := data.NewTuple(int64(a[0].(int)), make([]driver.Value, len(a)-1))
	for i := 1; i < len(a); i++ {
		switch a[i].(type) {
		case int:
			res.SetData(i-1, 0, int64(a[i].(int)))
		case float32:
			res.SetData(i-1, 0, float64(a[i].(float32)))
		default:
			res.SetData(i-1, 0, a[i])
		}
	}
	return res
}

func Test_createIdView(t *testing.T) {
	type args struct {
		left  *TableViewData
		right *TableViewData
	}
	empty := &TableViewData{cols: []int{}, tuples: []data.Tuple{}}
	tests := []struct {
		name string
		args args
		want [][]int64
	}{
		{
			name: "leftempty",
			args: args{
				empty,
				dv(v(1, 1))},
			want: [][]int64{},
		},
		{
			name: "rightempty",
			args: args{
				dv(v(1, 1)),
				empty},
			want: [][]int64{},
		},
		{
			name: "onlyonematching",
			args: args{
				dv(v(1, 1)),
				dv(v(1, 1))},
			want: [][]int64{{1, 1}},
		},
		{
			name: "onlytwomatching",
			args: args{
				dv(v(1, 1), v(2, 2)),
				dv(v(1, 1), v(2, 2))},
			want: [][]int64{{1, 1}, {2, 2}},
		},
		{
			name: "twomatchingwithm2n",
			args: args{
				dv(v(1, 1), v(2, 1), v(3, 2)),
				dv(v(1, 1), v(2, 2))},
			want: [][]int64{{1, 1}, {2, 1}, {3, 2}},
		},
		{
			name: "twomatchingwithm2nx2",
			args: args{
				dv(v(1, 1), v(2, 1), v(3, 2), v(4, 3), v(5, 3), v(6, 4)),
				dv(v(1, 1), v(2, 2), v(3, 3), v(4, 4))},
			want: [][]int64{{1, 1}, {2, 1}, {3, 2}, {4, 3}, {5, 3}, {6, 4}},
		},
		{
			name: "twomatchingwithm2nvize",
			args: args{
				dv(v(1, 1), v(2, 2)),
				dv(v(1, 1), v(2, 1), v(3, 2))},
			want: [][]int64{{1, 1}, {1, 2}, {2, 3}},
		},
		{
			name: "multipleleftmatchingwithmultipleright",
			args: args{
				dv(v(1, 1), v(2, 1), v(3, 2)),
				dv(v(1, 1), v(2, 1), v(3, 1), v(4, 3))},
			want: [][]int64{{1, 1}, {1, 2}, {1, 3}, {2, 1}, {2, 2}, {2, 3}},
		},
		{
			name: "multipleleftmatchingwithmultipleright",
			args: args{
				dv(v(0, 0), v(1, 1), v(2, 1), v(3, 2)),
				dv(v(1, 1), v(2, 1), v(3, 1), v(4, 3))},
			want: [][]int64{{1, 1}, {1, 2}, {1, 3}, {2, 1}, {2, 2}, {2, 3}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t2int64 := func(t [][]data.Tuple) [][]int64 {
				var res [][]int64
				for _, ts := range t {
					row := make([]int64, len(ts))
					for ti, t := range ts {
						row[ti] = t.Id()
					}
					res = append(res, row)
				}
				return res
			}
			res := createIdView(tt.args.left, tt.args.right)
			assert.Equalf(t, tt.want, t2int64(res), "createIdView(%v, %v)", tt.args.left, tt.args.right)
		})
	}
}

func Test_lessThan(t *testing.T) {
	type args struct {
		a data.Tuple
		b data.Tuple
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{name: "1", args: args{v(1, 1), v(2, 2)}, want: true},
		{name: "1", args: args{v(1, 1), v(2, 1)}, want: false},
		{name: "1", args: args{v(1, 2), v(2, 1)}, want: false},
		{name: "1", args: args{v(2, 1), v(1, 2)}, want: true},
		{name: "1", args: args{v(2, 1), v(1, 1)}, want: false},
		{name: "1", args: args{v(2, 2), v(1, 1)}, want: false},
		{name: "1", args: args{v(2, 1, 3), v(1, 2, 3)}, want: true},
		{name: "1", args: args{v(2, 1, 4), v(1, 1, 4)}, want: false},
		{name: "1", args: args{v(2, 2, 5), v(1, 1, 5)}, want: false},
		{name: "1", args: args{v(2, 1, 3), v(2, 1, 4)}, want: true},
		{name: "1", args: args{v(2, 1, 4), v(2, 1, 4)}, want: false},
		{name: "1", args: args{v(2, 2, 5), v(2, 2, 3)}, want: false},
		{name: "1", args: args{v(1, 1.1), v(2, 2.1)}, want: true},
		{name: "1", args: args{v(1, 1.1), v(2, 1.1)}, want: false},
		{name: "1", args: args{v(1, 2.1), v(2, 1.1)}, want: false},
		{name: "1", args: args{v(2, 1.1), v(1, 2.1)}, want: true},
		{name: "1", args: args{v(2, 1.1), v(1, 1.1)}, want: false},
		{name: "1", args: args{v(2, 2.1), v(1, 1.1)}, want: false},
		{name: "1", args: args{v(1, "1.1"), v(2, "2.1")}, want: true},
		{name: "1", args: args{v(1, "1.1"), v(2, "1.1")}, want: false},
		{name: "1", args: args{v(1, "2.1"), v(2, "1.1")}, want: false},
		{name: "1", args: args{v(2, "1.1"), v(1, "2.1")}, want: true},
		{name: "1", args: args{v(2, "1.1"), v(1, "1.1")}, want: false},
		{name: "1", args: args{v(2, "2.1"), v(1, "1.1")}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, lessThan(dv(tt.args.a), dv(tt.args.b), tt.args.a, tt.args.b), "lessThan(%v, %v)", tt.args.a, tt.args.b)
		})
	}
}
