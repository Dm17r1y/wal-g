package pgbackrest

import (
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"os"
	"path"
	"path/filepath"
)

func HandlePgbackrestBackupFetch(folder storage.Folder, stanza string, destinationDirectory string,
	backupSelector internal.BackupSelector) error {
	backupName, err := backupSelector.Select(folder)
	if err != nil {
		return err
	}

	backupDetails, err := GetBackupDetails(folder, stanza, backupName)
	if err != nil {
		return err
	}
	backupFilesFolder := folder.GetSubFolder(BackupFolderName).GetSubFolder(stanza).GetSubFolder(backupName).GetSubFolder(BackupDataDirectory)
	fileExtractor := internal.NewRawFileExteractor(destinationDirectory)
	files, err := getFilesRecursively(backupFilesFolder, backupFilesFolder)
	if err != nil {
		return err
	}
	err = internal.ExtractAll(fileExtractor, files)
	if err != nil {
		return err
	}
	return restoreDirectories(*backupDetails, destinationDirectory)
}

func restoreDirectories(backupDetails BackupDetails, destinationDirectory string) error {
	for _, directoryPath := range backupDetails.DirectoryPaths {
		relativeDirectory, err := filepath.Rel(BackupDataDirectory, directoryPath)
		if err != nil {
			return err
		}
		err = os.MkdirAll(filepath.Join(destinationDirectory, relativeDirectory), 0755)
		if err != nil {
			return err
		}
	}
	return os.Chmod(destinationDirectory, 0700)
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
