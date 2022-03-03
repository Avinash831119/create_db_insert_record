package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"kredit_bee_project/util"
	"os"
)

func main() {
	database, err := util.Connection()

	if err != nil {
		fmt.Println("database_creation:main, unable to get connection ", err.Error())
	}

	stmt, _ := database.Prepare("CREATE TABLE `avinash.verma1983@gmail.com.album` (  `id` int(11) NOT NULL AUTO_INCREMENT,  `userId` int(11) NOT NULL,  `title `varchar(150) NOT NULL,  PRIMARY KEY (`id`)) ")
	_, err = stmt.Exec()
	if err != nil {
		fmt.Println("unable to create Album table", err.Error())
	} else {
		fmt.Println("Album table created successfully")

		albumErr := insertRecordIntoAlbumTable(database)
		if albumErr != nil {
			fmt.Println("unable to insert record into Album table", albumErr.Error())
		}

		fmt.Println("Data inserted into Album table successfully")
	}

	statment, _ := database.Prepare("CREATE TABLE `avinash.verma1983@gmail.com.photo` (   `id` int(11) NOT NULL AUTO_INCREMENT,  `albumId` int(11) NOT NULL,  `photoId` int(11) NOT NULL, `title` varchar(150) NOT NULL,  `url` varchar(500) NOT NULL,  `thumbnailUrl` varchar(500) NOT NULL,  PRIMARY KEY (`id`)) ")
	_, err = statment.Exec()
	if err != nil {
		fmt.Println("unable to create photo table ", err.Error())
	} else {
		fmt.Println("Photo table created successfully")

		err := insertRecordIntoPhotoTable(database)

		if err != nil {
			fmt.Println("unable to insert record into Photo table", err.Error())
		}

		fmt.Println("Data inserted into Photo table successfully")
	}

}

func insertRecordIntoPhotoTable(database *sql.DB) error {
	records, err := readData("C:/Code/go_workspace/kredit_bee_project/create_database/photo.csv")
	if err != nil {
		fmt.Errorf("unable to read data file", err.Error())
		return err
	}

	for _, record := range records {
		sqlStatement := `INSERT INTO avinash.verma1983@gmail.com.photo ( albumId, photoId, title, url, thumbnailUrl) VALUES ($1, $2, $3, $4, $5)`
		_, err = database.Exec(sqlStatement, record[0], record[1], record[2], record[3], record[4])
		if err != nil {
			fmt.Errorf("unable to insert data file", err.Error())
			return err
		}
	}

	fmt.Printf("Data insert into photo table successfully")

	return nil
}

func insertRecordIntoAlbumTable(database *sql.DB) error {
	records, err := readData("C:/Code/go_workspace/kredit_bee_project/create_database/album.csv")
	if err != nil {
		fmt.Errorf("unable to read data file", err.Error())
		return err
	}

	for _, record := range records {
		sqlStatement := `INSERT INTO avinash.verma1983@gmail.com.album ( userId, title) VALUES ($1, $2)`
		_, err = database.Exec(sqlStatement, record[0], record[1])
		if err != nil {
			fmt.Errorf("unable to insert data file", err.Error())
			return err
		}
	}

	fmt.Printf("Data insert into Album table successfully")
	return nil
}

func readData(fileName string) ([][]string, error) {

	f, err := os.Open(fileName)

	if err != nil {
		return [][]string{}, err
	}

	defer f.Close()

	r := csv.NewReader(f)

	// skip first line
	if _, err := r.Read(); err != nil {
		return [][]string{}, err
	}

	records, err := r.ReadAll()

	if err != nil {
		return [][]string{}, err
	}

	return records, nil
}
