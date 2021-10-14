package pgbackrest

import (
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"path"
	"path/filepath"
)

func HandlePgbackrestBackupFetch(folder storage.Folder, stanza string, destinationDirectory string,
	backupSelector internal.BackupSelector) error {
	backupName, err := backupSelector.Select(folder)
	if err != nil {
		return err
	}

	backupFilesFolder := folder.GetSubFolder("backup").GetSubFolder(stanza).GetSubFolder(backupName).GetSubFolder("pg_data")
	fileExtractor := internal.NewRawFileExteractor(destinationDirectory)
	files, err := getFilesRecursively(backupFilesFolder, backupFilesFolder)
	if err != nil {
		return err
	}
	return internal.ExtractAll(fileExtractor, files)
}

func getFilesRecursively(folder storage.Folder, backupFilesFolder storage.Folder) (files []internal.ReaderMaker, err error) {
	objects, subfolders, err := folder.ListFolder()
	if err != nil {
		return nil, err
	}

	for _, object := range objects {
		relativePath, err := filepath.Rel(backupFilesFolder.GetPath(), folder.GetPath())
		if err != nil {
			return nil, err
		}
		file := internal.NewStorageReaderMaker(backupFilesFolder, path.Join(relativePath, object.GetName()))
		files = append(files, file)
	}

	for _, subfolder := range subfolders {
		subfolderFiles, err := getFilesRecursively(subfolder, backupFilesFolder)
		if err != nil {
			return nil, err
		}
		for _, subfolderFile := range subfolderFiles {
			files = append(files, subfolderFile)
		}
	}
	return files, err
}
