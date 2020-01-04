package datamanager

import (
	"GoConcurrency/distributed/dto"
	"errors"
)

var sensors map[string]int

func SaveReader(reading *dto.SensorMessage) error {
	if sensors[reading.Name] == 0 {
		getSensors()
	}
	if sensors[reading.Name] == 0 {
		return errors.New("Unable to find sensor for name '" + reading.Name + "'")

	}
	q := `insert into sensor_reading (value, sensor_id, taken_on) values ($1, $2, $3)`
	_, err := db.Exec(q, reading.Value, sensors[reading.Name], reading.Timestamp)
	return err
}

func getSensors() {
	sensors = make(map[string]int)
	q := `select id, name from sensor`
	rows, _ := db.Query(q)

	for rows.Next() {
		var id int
		var name string
		rows.Scan(&id, &name)

		sensors[name] = id
	}

}
