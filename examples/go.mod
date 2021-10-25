module github.com/cloudspannerecosystem/go-sql-spanner/examples

go 1.14

replace github.com/cloudspannerecosystem/go-sql-spanner => ../

require (
	cloud.google.com/go v0.97.0
	cloud.google.com/go/spanner v1.27.0
	github.com/cloudspannerecosystem/go-sql-spanner v0.0.0-00010101000000-000000000000
	github.com/containerd/containerd v1.5.5 // indirect
	github.com/docker/docker v20.10.8+incompatible
	github.com/docker/go-connections v0.4.0
	github.com/gorilla/mux v1.8.0 // indirect
	github.com/moby/term v0.0.0-20210619224110-3f7ff695adc6 // indirect
	github.com/morikuni/aec v1.0.0 // indirect
	google.golang.org/genproto v0.0.0-20211019152133-63b7e35f4404
)
