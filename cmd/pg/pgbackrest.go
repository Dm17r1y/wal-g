package pg

import (
	"errors"
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const pgbackrestCommandDescription = "Work with pgbackrest backups"

var pgbackrestCmd = &cobra.Command{
	Use:   "pgbackrest",
	Short: pgbackrestCommandDescription,
}

func init() {
	Cmd.AddCommand(pgbackrestCmd)
}

func configurePgbackrestSettings() (folder storage.Folder, stanza string) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)
	stanza, ok := internal.GetSetting(internal.PgbackrestStanza)
	if !ok {
		tracelog.ErrorLogger.FatalError(errors.New("stanza is not set"))
	}

	return folder, stanza
}
