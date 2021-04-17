# mysqlerr

MySQL Server Error Constants.

Covers up to MySQL 8.0.19. Notice that some constants were renamed in later
versions of MySQL, because they became obsolete. (In case you wonder: the names
here match the symbols MySQL uses in source code.) Obsolete names haven't been
changed in this package to avoid breaking code, but you should no longer be
using them in applications. New and/or changed names (symbols) are given in
inline comments.

```go
const (
    ER_HASHCHK                  = 1000 // OBSOLETE_ER_HASHCHK
    ER_NISAMCHK                 = 1001 // OBSOLETE_ER_NISAMCHK
    // ...
    ER_WRONG_OUTER_JOIN         = 1120 // ER_WRONG_OUTER_JOIN_UNUSED
    // ...
    ER_DELAYED_CANT_CHANGE_LOCK = 1150 // OBSOLETE_ER_UNUSED1
    ER_TOO_MANY_DELAYED_THREADS = 1151 // OBSOLETE_ER_UNUSED2
    // ...
)
```
