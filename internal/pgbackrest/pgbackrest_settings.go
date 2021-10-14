package pgbackrest

import (
	"encoding/json"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"gopkg.in/ini.v1"
)

const (
	BackupPath        = "backup"
	BackupInfoIni     = "backup.info"
	BackupManifestIni = "backup.manifest"
)

type BackupSettings struct {
	Name     string
	Settings PgbackrestBackupSettings
}

type PgbackrestBackupSettings struct {
	BackrestFormat          int    `json:"backrest-format"`
	BackrestVersion         string `json:"backrest-version"`
	BackupInfoRepoSize      int64  `json:"backup-info-repo-size"`
	BackupInfoRepoSizeDelta int64  `json:"backup-info-repo-size-delta"`
	BackupInfoSize          int64  `json:"backup-info-size"`
	BackupInfoSizeDelta     int64  `json:"backup-info-size-delta"`
	BackupPgId              int    `json:"db-id"`

	BackupTimestampStart int64  `json:"backup-timestamp-start"`
	BackupTimestampStop  int64  `json:"backup-timestamp-stop"`
	BackupType           string `json:"backup-type"`

	BackupArchiveStart string   `json:"backup-archive-start"`
	BackupArchiveStop  string   `json:"backup-archive-stop"`
	BackupPrior        string   `json:"backup-prior"`
	BackupReference    []string `json:"backup-reference"`

	OptionArchiveCheck  bool `json:"option-archive-check"`
	OptionArchiveCopy   bool `json:"option-archive-copy"`
	OptionBackupStandby bool `json:"option-backup-standby"`
	OptionChecksumPage  bool `json:"option-checksum-page"`
	OptionCompress      bool `json:"option-compress"`
	OptionHardlink      bool `json:"option-hardlink"`
	OptionOnline        bool `json:"option-online"`
}

type PgbackrestSection struct {
	BackrestFormant string `ini:"backrest-format"`
	BackrestVersion string `ini:"backrest-version"`
}

type BackupSection struct {
	BackupArchiveStart       string `ini:"backup-archive-start"`
	BackupArchiveStop        string `ini:"backup-archive-stop"`
	BackupLabel              string `ini:"backup-label"`
	BackupLabelPrior         string `ini:"backup-prior"`
	BackupLsnStart           string `ini:"backup-lsn-start"`
	BackupLsnStop            string `ini:"backup-lsn-stop"`
	BackupTimestampCopyStart int64  `ini:"backup-timestamp-copy-start"`
	BackupTimestampStart     int64  `ini:"backup-timestamp-start"`
	BackupTimestampStop      int64  `ini:"backup-timestamp-stop"`
	BackupType               string `ini:"backup-type"`
}

type BackupTargetSection struct {
	PgdataPath string
}

type PgbackrestManifestSettings struct {
	BackrestSection       PgbackrestSection     `ini:"backrest"`
	BackupSection         BackupSection         `ini:"backup"`
	BackupTargetSection   BackupTargetSection   `ini:"backup:target"`
	BackupDatabaseSection BackupDatabaseSection `ini:"backup:db"`
}

type BackupDatabaseSection struct {
	CatalogVersion uint64 `ini:"db-catalog-version"`
	ControlVersion uint64 `ini:"db-control-version"`
	Id             uint64 `ini:"db-id"`
	SystemId       uint64 `ini:"db-system-id"`
	Version        string `ini:"db-version"`
}

type PgData struct {
	Path     string `json:"path"`
	PathType string `json:"type"`
}

func LoadBackupsSettings(folder storage.Folder, stanza string) ([]BackupSettings, error) {
	backupFolder := folder.GetSubFolder(BackupPath).GetSubFolder(stanza)
	ioReader, err := backupFolder.ReadObject(BackupInfoIni)
	if err != nil {
		return nil, err
	}

	cfg, err := ini.Load(ioReader)
	if err != nil {
		return nil, err
	}

	backupSection, err := cfg.GetSection("backup:current")
	if err != nil {
		return nil, err
	}

	var backupsSettings []BackupSettings
	for _, key := range backupSection.Keys() {
		var settings PgbackrestBackupSettings
		if err := json.Unmarshal([]byte(key.Value()), &settings); err != nil {
			return nil, err
		}

		backupSettings := BackupSettings{
			Name:     key.Name(),
			Settings: settings,
		}
		backupsSettings = append(backupsSettings, backupSettings)
	}

	return backupsSettings, nil
}

func LoadManifest(folder storage.Folder, stanza string, backupName string) (*PgbackrestManifestSettings, error) {
	backupFolder := folder.GetSubFolder(BackupPath).GetSubFolder(stanza).GetSubFolder(backupName)
	ioReader, err := backupFolder.ReadObject(BackupManifestIni)
	if err != nil {
		return nil, err
	}
	cfg, err := ini.Load(ioReader)
	if err != nil {
		return nil, err
	}
	var settings PgbackrestManifestSettings
	if err := cfg.MapTo(&settings); err != nil {
		return nil, err
	}
	return &settings, nil
}


