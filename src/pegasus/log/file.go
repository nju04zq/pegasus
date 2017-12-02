package log

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type FileLogger struct {
	Path       string
	RotateSize int64
	Level      int
	realPath   string
	mutex      sync.Mutex
	file       *os.File
	size       int64
}

func (l *FileLogger) ts() string {
	return time.Now().Format("2006-01-02T15:04:05.000")
}

func (l *FileLogger) init() error {
	if path, err := filepath.Abs(l.Path); err != nil {
		return fmt.Errorf("Fail to get Abs path from %q, %v", l.Path, err)
	} else {
		l.Path = path
	}
	_, err := os.Stat(l.Path)
	if err == nil {
		return l.initFromExisted()
	} else if os.IsNotExist(err) {
		return l.initFromNonExisted()
	} else {
		return fmt.Errorf("Fail to stat %q, %v", l.Path, err)
	}
	return nil
}

func (l *FileLogger) initFromNonExisted() error {
	l.realPath = fmt.Sprintf("%s[%s", l.Path, l.ts())
	if err := l.initFile(); err != nil {
		return err
	}
	if err := os.Symlink(l.realPath, l.Path); err != nil {
		return fmt.Errorf("Fail to ln -s %q %q, %v", l.realPath, l.Path, err)
	}
	return nil
}

func (l *FileLogger) initFromExisted() error {
	if err := l.validateExistedLogPath(l.Path); err != nil {
		return err
	}
	path, err := os.Readlink(l.Path)
	if err != nil {
		return fmt.Errorf("Fail to Readlink %q, %v", l.Path, err)
	}
	l.realPath = path
	if err := l.initFile(); err != nil {
		return err
	}
	return nil
}

func (l *FileLogger) initFile() error {
	path := l.realPath
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("Fail to open %q, %v", path, err)
	}
	l.file = f
	finfo, err := f.Stat()
	if err != nil {
		return fmt.Errorf("Fail to stat %q, %v", path, err)
	}
	l.size = finfo.Size()
	return nil
}

func (l *FileLogger) validateExistedLogPath(path string) error {
	finfo, err := os.Lstat(l.Path)
	if err != nil {
		return fmt.Errorf("Fail to Lstat %q, %v", l.Path, err)
	}
	if (finfo.Mode() & os.ModeSymlink) == 0 {
		return fmt.Errorf("Log path %q exists as non-symlink", l.Path)
	}
	return nil
}

func (l *FileLogger) getLevel() int {
	return l.Level
}

func (l *FileLogger) write(s string) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.file.WriteString(s)
	l.size += int64(len(s))
	if l.size >= l.RotateSize {
		l.rotate()
	}
}

func (l *FileLogger) finalPath() string {
	return fmt.Sprintf("%s,%s]", l.realPath, l.ts())
}

func (l *FileLogger) rotate() {
	l.file.Close()
	os.Rename(l.realPath, l.finalPath())
	os.Remove(l.Path)
	if err := l.initFromNonExisted(); err != nil {
		panic(fmt.Errorf("Fail to init after rotate, %v", err))
	}
}
