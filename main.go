package main

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/joho/godotenv"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq"
)

const (
	UPCOMING_APPOINTMENT = "UPCOMING_APPOINTMENT"
	SEND_SMS             = "SEND_SMS"
)

func main() {
	//ctx, cancel := context.WithCancel(context.Background())
	//
	//interrupt := make(chan os.Signal, 1)
	//signal.Notify(interrupt, os.Interrupt)

	if err := godotenv.Load(); err != nil {
		log.Fatal("Error loading .env file ", err)
	}

	db := initDBConnection()

	go func() {
		op := "checkAppointments"
		for true {
			log.Println(op)

			appointments, err := getAppointments(op, db)
			if err != nil {
				log.Println(err)
			}

			if err := addJobs(op, db, appointments, UPCOMING_APPOINTMENT); err != nil {
				log.Println(err)
			}

			time.Sleep(2 * time.Second)
		}
	}()

	go func() {
		op := "publishNotifications"
		for true {
			log.Println(op)

			jobs, err := getJobs(op, db, UPCOMING_APPOINTMENT)
			if err != nil {
				log.Println(err)
			}

			if err := addNotifications(op, db, jobs); err != nil {
				log.Println(err)
			}

			if err := markJobsCompleted(op, db, jobs); err != nil {
				log.Println(err)
			}

			time.Sleep(2 * time.Second)
		}
	}()

	go func() {
		op := "checkNotifications"
		for true {
			log.Println(op)

			notifications, err := getNotifications(op, db)
			if err != nil {
				log.Println(err)
			}

			if err := addJobs2(op, db, notifications, SEND_SMS); err != nil {
				log.Println(err)
			}

			time.Sleep(2 * time.Second)
		}
	}()

	go func() {
		op := "publishSMS"
		for true {
			log.Println(op)
			jobs, err := getJobs(op, db, SEND_SMS)
			if err != nil {
				log.Println(err)
			}

			if err := sendSms(op, db, jobs); err != nil {
				log.Println(err)
			}

			if err := markNotificationSentTime(op, db, jobs); err != nil {
				log.Println(err)
			}

			if err := markJobsCompleted(op, db, jobs); err != nil {
				log.Println(err)
			}

			time.Sleep(2 * time.Second)

		}
	}()

	for true {
	} // keep alive

}

func getAppointments(op string, db *sql.DB) ([]Appointment, error) {
	rows, err := db.Query(`
SELECT a.id, a.dealership_id, a.scheduled_for
from appointments a
left join jobs j on a.id = (j.payload ->> 'id')::int
      and j.name = $1
where a.scheduled_for > current_timestamp
  and j.id is null`, UPCOMING_APPOINTMENT)
	if err != nil {
		return nil, fmt.Errorf("error querying for appointments: %v", err)
	}

	var appts []Appointment
	for rows.Next() {
		appt := Appointment{}
		if err := rows.Scan(&appt.Id, &appt.DealershipId, &appt.ScheduledFor); err != nil {
			return nil, fmt.Errorf("error scanning for appointments: %v", err)
		}
		log.Println(op, "found ", appt)
		appts = append(appts, appt)
	}

	return appts, nil
}

func addJobs(op string, db *sql.DB, appointments []Appointment, jobName string) error {
	for _, appt := range appointments {
		runTimes := getRunTimes(appt)
		for _, runTime := range runTimes {
			json, err := json.Marshal(appt)
			if err != nil {
				return fmt.Errorf("cannot marshal json for %v: %v", appt, err)
			}
			log.Println(op, "inserting into jobs ", appt, " at scheduled time ", runTime)
			_, err = db.Exec(`INSERT INTO jobs (name, payload, "runAt") VALUES ($1, $2, $3)`, jobName, json, runTime)
			if err != nil {
				return fmt.Errorf("cannot insert into jobs: %v", err)
			}
			log.Println(op, "inserted")
		}
	}
	return nil
}

func addJobs2(op string, db *sql.DB, notifications []Notification, jobName string) error {
	for _, notification := range notifications {
		json, err := json.Marshal(notification)
		if err != nil {
			return fmt.Errorf("cannot marshal json for %v: %v", notification, err)
		}
		log.Println(op, "inserting into jobs ", notification)
		_, err = db.Exec(`INSERT INTO jobs (name, payload, "runAt") VALUES ($1, $2, CURRENT_TIMESTAMP)`, jobName, json)
		if err != nil {
			return fmt.Errorf("cannot insert into jobs: %v", err)
		}
		log.Println(op, "inserted")
	}
	return nil
}

func getJobs(op string, db *sql.DB, jobName string) ([]Job, error) {
	var jobs []Job
	rows, err := db.Query(`SELECT id, payload from jobs where name = $1 and "runAt" < current_timestamp and coalesce(status, '') != 'COMPLETED'`, jobName)
	if err != nil {
		return nil, fmt.Errorf("error querying jobs: %v", err)
	}

	for rows.Next() {
		job := Job{}
		err := rows.Scan(&job.Id, &job.Payload)
		if err != nil {
			return nil, fmt.Errorf("error scanning for jobs: %v", err)
		}
		log.Println(op, "found ", job)
		jobs = append(jobs, job)

	}
	return jobs, err
}

func addNotifications(op string, db *sql.DB, jobs []Job) error {
	for _, job := range jobs {
		dealershipId := job.Payload["dealership_id"].(string) // TODO: brittle, panics if field doesn't exist

		log.Println(op, "inserting into notifications ", job)
		_, err := db.Exec(
			`
INSERT INTO notifications (dealership_id, title, body, created_time) 
VALUES ($1, $2, $3, current_timestamp)`,
			dealershipId,
			"title",
			"body",
		)
		if err != nil {
			return fmt.Errorf("error inserting into notifications: %v", err)
		}
	}
	return nil
}

func markJobsCompleted(op string, db *sql.DB, jobs []Job) error {
	for _, job := range jobs {
		log.Println(op, "marking job completed for job ", job)
		_, err := db.Exec(`UPDATE jobs set status = 'COMPLETED' where id = $1`, job.Id)
		if err != nil {
			return fmt.Errorf("error updating jobs: %v", err)
		}
	}
	return nil
}

func markNotificationSentTime(op string, db *sql.DB, jobs []Job) error {
	for _, job := range jobs {
		log.Println(op, "marking notification sent for job ", job)
		_, err := db.Exec(`UPDATE notifications set sent_time = current_timestamp where id = $1`, job.Payload["id"])
		if err != nil {
			return fmt.Errorf("error updating notification: %v", err)
		}
	}
	return nil
}

func sendSms(op string, db *sql.DB, jobs []Job) error {
	for _, job := range jobs {
		log.Println(op, "PUBLISHING SMS!!", job)
	}
	return nil
}

func getNotifications(op string, db *sql.DB) ([]Notification, error) {
	rows, err := db.Query(`
SELECT n.id,
       n.dealership_id,
       n.user_id,
       n.type,
       n.title,
       n.body,
       n.created_time,
       n.sent_time,
       n.read_time
from notifications n
left join jobs j on n.id = (j.payload ->> 'id')::int
      and j.name = $1
where sent_time is null`, SEND_SMS)
	if err != nil {
		return nil, fmt.Errorf("error querying for notifications: %v", err)
	}

	var notifications []Notification
	for rows.Next() {
		notification := Notification{}
		if err := rows.Scan(
			&notification.Id,
			&notification.DealershipId,
			&notification.UserId,
			&notification.Type,
			&notification.Title,
			&notification.Body,
			&notification.CreatedTime,
			&notification.SentTime,
			&notification.ReadTime,
		); err != nil {
			return nil, fmt.Errorf("error scanning for notifications: %v", err)
		}
		log.Println(op, "found ", notification)
		notifications = append(notifications, notification)
	}

	return notifications, nil
}

func getRunTimes(appt Appointment) []time.Time {

	runTimes := []time.Time{time.Now()}

	dayBefore := appt.ScheduledFor.Add(-24 * time.Hour)
	if time.Now().Before(dayBefore) {
		runTimes = append(runTimes, dayBefore)
	}

	halfHourBefore := appt.ScheduledFor.Add(-30 * time.Minute)
	if time.Now().Before(halfHourBefore) {
		runTimes = append(runTimes, halfHourBefore)
	}

	return runTimes
}

func initDBConnection() *sql.DB {
	connStr := os.Getenv("DB_DSN")
	db, err := sql.Open("postgres", connStr)

	if err != nil {
		log.Panic("couldn't connect to database", err)
	}

	return db
}

type Appointment struct {
	Id           uint      `json:"id"`
	DealershipId string    `json:"dealership_id"`
	ScheduledFor time.Time `json:"scheduled_for"`
}

type Job struct {
	Id      uint       `db:"id"`
	Payload PayloadMap `db:"payload"`
}

type Notification struct {
	Id           uint    `db:"id"            json:"id"`
	DealershipId string  `db:"dealership_id" json:"dealership_id"`
	UserId       *string `db:"user_id"       json:"user_id"`
	Type         *string `db:"type"          json:"type"`
	Title        string  `db:"title"         json:"title"`
	Body         string  `db:"body"          json:"body"`
	CreatedTime  string  `db:"created_time"  json:"created_time"`
	SentTime     *string `db:"sent_time"     json:"sent_time"`
	ReadTime     *string `db:"read_time"     json:"read_time"`
}

type PayloadMap map[string]interface{}

func (p PayloadMap) Value() (driver.Value, error) {
	return json.Marshal(p)
}

func (p *PayloadMap) Scan(src interface{}) error {
	source, ok := src.([]byte)
	if !ok {
		return errors.New("Type assertion .([]byte) failed.")
	}

	var i interface{}
	err := json.Unmarshal(source, &i)
	if err != nil {
		return err
	}

	*p, ok = i.(map[string]interface{})
	if !ok {
		return errors.New("Type assertion .(map[string]interface{}) failed.")
	}

	return nil
}
