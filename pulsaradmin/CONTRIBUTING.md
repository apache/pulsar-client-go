# Contributing guidelines

## Project structure

The overall project structure is illustrated below:

```text
├── pkg/
│   ├── admin/
│   │   ├── auth/
│   │   ├── config/
│   ├── rest/
│   └── utils/
└── alias.go
```

- The `alias.go` file in the root defines `pulsaradmin` package scope, which contains shortcuts of some types and functions from the `pkg`. 
- The `pkg/admin` package contains all operations for pulsar admin resources. *Note: We should add a new file here if we wanna support a new resource.*
  - The `pkg/admin/config` package contains configuration options for constructing a pulsar admin client.
  - The `pkg/admin/auth` package contains auth providers which work in transport layer. 
- The `pkg/rest` package contains a wrapped HTTP client for requesting pulsar REST API.
- The `pkg/utils` package contains common data structures and functions.

## Contributing steps
1. Submit an issue describing your proposed change.
2. Discuss and wait for proposal to be accepted.
3. Fork this repo, develop and test your code changes.
4. Submit a pull request.

## Conventions

Please read through below conventions before contributions.

### PullRequest conventions

- Use [Conventional Commits specification](https://www.conventionalcommits.org/en/v1.0.0/) to standardize PR title.

### Code conventions

- [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments)
- [Effective Go](https://golang.org/doc/effective_go.html)
- Know and avoid [Go landmines](https://gist.github.com/lavalamp/4bd23295a9f32706a48f)
- Commenting
  - [Go's commenting conventions](http://blog.golang.org/godoc-documenting-go-code)
  - If reviewers ask questions about why the code is the way it is, that's a sign that comments might be helpful.
- Naming
  - Please consider package name when selecting an interface name, and avoid redundancy. For example, `storage.Interface` is better than `storage.StorageInterface`.
  - Do not use uppercase characters, underscores, or dashes in package names.
  - Please consider parent directory name when choosing a package name. For example, `pkg/controllers/autoscaler/foo.go` should say `package autoscaler` not `package autoscalercontroller`.
      - Unless there's a good reason, the `package foo` line should match the name of the directory in which the `.go` file exists.
      - Importers can use a different name if they need to disambiguate.
  - Locks should be called `lock` and should never be embedded (always `lock sync.Mutex`). When multiple locks are present, give each lock a distinct name following Go conventions: `stateLock`, `mapLock` etc.
  
### Folder and file conventions

- All filenames should be lowercase.
- Go source files and directories use underscores, not dashes.
  - Package directories should generally avoid using separators as much as possible. When package names are multiple words, they usually should be in nested subdirectories.
- Document directories and filenames should use dashes rather than underscores.
- All source files should add a license at the beginning.