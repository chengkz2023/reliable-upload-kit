package reliableupload

import "errors"

// ErrAlreadyExists indicates repository persistence conflict on a unique key.
// Engine treats this as idempotent conflict and can continue safely.
var ErrAlreadyExists = errors.New("already exists")
