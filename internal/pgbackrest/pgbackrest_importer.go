package pgbackrest

import (
	"fmt"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"time"
)

type BackupDetails struct {
	BackupName       string
	ModifiedTime     time.Time
	WalFileName      string
	Type             string
	StartTime        time.Time
	FinishTime       time.Time
	PgVersion        string
	StartLsn         uint64
	FinishLsn        uint64
	SystemIdentifier uint64
}

func GetBackupList(backupsFolder storage.Folder, stanza string) ([]internal.BackupTime, error) {
	backupsSettings, err := LoadBackupsSettings(backupsFolder, stanza)
	if err != nil {
		return nil, err
	}

	var backupTimes []internal.BackupTime
	for _, backupSettings := range backupsSettings {
		backupTimes = append(backupTimes, internal.BackupTime{
			BackupName:  backupSettings.Name,
			Time:        getTime(backupSettings.Settings.BackupTimestampStop),
			WalFileName: backupSettings.Settings.BackupArchiveStart,
		})
	}
	return backupTimes, nil
}

func GetBackupDetails(backupsFolder storage.Folder, stanza string, backupName string) (*BackupDetails, error) {
	manifest, err := LoadManifest(backupsFolder, stanza, backupName)
	if err != nil {
		return nil, err
	}

	backupTime := internal.BackupTime{
		BackupName:  manifest.BackupSection.BackupLabel,
		Time:        getTime(manifest.BackupSection.BackupTimestampStop),
		WalFileName: manifest.BackupSection.BackupArchiveStart,
	}

	startLsn, err := getLsn(manifest.BackupSection.BackupLsnStart)
	if err != nil {
		return nil, err
	}

	finishLsn, err := getLsn(manifest.BackupSection.BackupLsnStop)
	if err != nil {
		return nil, err
	}

	backupDetails := BackupDetails{
		BackupName:       backupTime.BackupName,
		ModifiedTime:     backupTime.Time,
		WalFileName:      backupTime.WalFileName,
		Type:             manifest.BackupSection.BackupType,
		StartTime:        getTime(manifest.BackupSection.BackupTimestampStart),
		FinishTime:       getTime(manifest.BackupSection.BackupTimestampStop),
		PgVersion:        manifest.BackupDatabaseSection.Version,
		StartLsn:         startLsn,
		FinishLsn:        finishLsn,
		SystemIdentifier: manifest.BackupDatabaseSection.SystemId,
	}

	return &backupDetails, nil
}

func getTime(timestamp int64) time.Time {
	return time.Unix(timestamp, 0)
}

func getLsn(lsn string) (uint64, error) {
	var first uint64
	var second uint64
	if _, err := fmt.Sscanf(lsn, "%x/%x", &first, &second); err != nil {
		return 0, err
	}
	return (first << 32) + second, nil
}
