package parser

import (
	"database/sql/driver"
	"github.com/stretchr/testify/assert"
	"testing"
)

func dv(v ...[]driver.Value) [][]driver.Value {
	res := make([][]driver.Value, len(v))
	for i := range v {
		res[i] = v[i]
	}
	return res
}

func v(a ...driver.Value) []driver.Value {
	res := make([]driver.Value, len(a))
	for i := 0; i < len(a); i++ {
		switch a[i].(type) {
		case int:
			res[i] = int64(a[i].(int))
		case float32:
			res[i] = float64(a[i].(float32))
		default:
			res[i] = a[i]
		}
	}
	return res
}

func Test_createIdView(t *testing.T) {
	type args struct {
		left  *JoinViewData
		right *JoinViewData
	}
	empty := dv()
	tests := []struct {
		name string
		args args
		want [][]int64
	}{
		{
			name: "leftempty",
			args: args{
				&JoinViewData{nil, false, empty},
				&JoinViewData{nil, false, dv(v(1, 1))}},
			want: [][]int64{},
		},
		{
			name: "rightempty",
			args: args{
				&JoinViewData{nil, false, dv(v(1, 1))},
				&JoinViewData{nil, false, empty}},
			want: [][]int64{},
		},
		{
			name: "onlyonematching",
			args: args{
				&JoinViewData{nil, false, dv(v(1, 1))},
				&JoinViewData{nil, false, dv(v(1, 1))}},
			want: [][]int64{{1, 1}},
		},
		{
			name: "onlytwomatching",
			args: args{
				&JoinViewData{nil, false, dv(v(1, 1), v(2, 2))},
				&JoinViewData{nil, false, dv(v(1, 1), v(2, 2))}},
			want: [][]int64{{1, 1}, {2, 2}},
		},
		{
			name: "twomatchingwithm2n",
			args: args{
				&JoinViewData{nil, false, dv(v(1, 1), v(2, 1), v(3, 2))},
				&JoinViewData{nil, false, dv(v(1, 1), v(2, 2))}},
			want: [][]int64{{1, 1}, {2, 1}, {3, 2}},
		},
		{
			name: "twomatchingwithm2nx2",
			args: args{
				&JoinViewData{nil, false, dv(v(1, 1), v(2, 1), v(3, 2), v(4, 3), v(5, 3), v(6, 4))},
				&JoinViewData{nil, false, dv(v(1, 1), v(2, 2), v(3, 3), v(4, 4))}},
			want: [][]int64{{1, 1}, {2, 1}, {3, 2}, {4, 3}, {5, 3}, {6, 4}},
		},
		{
			name: "twomatchingwithm2nvize",
			args: args{
				&JoinViewData{nil, false, dv(v(1, 1), v(2, 2))},
				&JoinViewData{nil, false, dv(v(1, 1), v(2, 1), v(3, 2))}},
			want: [][]int64{{1, 1}, {1, 2}, {2, 3}},
		},
		{
			name: "multipleleftmatchingwithmultipleright",
			args: args{
				&JoinViewData{nil, false, dv(v(1, 1), v(2, 1), v(3, 2))},
				&JoinViewData{nil, false, dv(v(1, 1), v(2, 1), v(3, 1), v(4, 3))}},
			want: [][]int64{{1, 1}, {1, 2}, {1, 3}, {2, 1}, {2, 2}, {2, 3}},
		},
		{
			name: "multipleleftmatchingwithmultipleright",
			args: args{
				&JoinViewData{nil, false, dv(v(0, 0), v(1, 1), v(2, 1), v(3, 2))},
				&JoinViewData{nil, false, dv(v(1, 1), v(2, 1), v(3, 1), v(4, 3))}},
			want: [][]int64{{1, 1}, {1, 2}, {1, 3}, {2, 1}, {2, 2}, {2, 3}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, createIdView(tt.args.left, tt.args.right), "createIdView(%v, %v)", tt.args.left, tt.args.right)
		})
	}
}

func Test_lessThan(t *testing.T) {
	type args struct {
		a []driver.Value
		b []driver.Value
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
			assert.Equalf(t, tt.want, lessThan(tt.args.a, tt.args.b), "lessThan(%v, %v)", tt.args.a, tt.args.b)
		})
	}
}
