package process

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"judaro13/miaguila/bulkapiservice/models"
	"judaro13/miaguila/bulkapiservice/utils"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/avast/retry-go"
	"github.com/streadway/amqp"
)

// Data process data for API query
func Data(rabbit *amqp.Connection, data []byte) error {
	message := models.QueryCoordinatesMessage{}
	err := json.Unmarshal(data, &message)
	if err != nil {
		return err
	}

	err = queryBulkData(rabbit, message.Coordinates, message.Reference)
	if err != nil {
		return err
	}

	return nil
}

func queryBulkData(rabbit *amqp.Connection, coords [][]string, reference string) error {
	query := stringCoordsToQueryStruct(coords)

	body, err := json.Marshal(query)
	if err != nil {
		utils.Error(err)
		return err
	}

	APIUrl := "https://api.postcodes.io/postcodes"

	var bodyResp []byte
	var result models.UKAPIPOSTResult

	err = retry.Do(
		func() error {
			bodyIO := bytes.NewBuffer(body)
			client := http.Client{Timeout: 10 * time.Second}
			resp, err := client.Post(APIUrl, "application/json", bodyIO)
			if err != nil {
				return err
			}

			if resp.StatusCode != http.StatusOK {
				err := errors.New(string(body))
				return err
			}

			defer resp.Body.Close()
			bodyResp, err = ioutil.ReadAll(resp.Body)
			if err != nil {
				return err
			}

			err = json.Unmarshal(bodyResp, &result)
			if err != nil {
				utils.Error(err)
				return err
			}

			if result.Status == 200 && len(result.Result) == 0 {
				return errors.New("no results")
			}
			return nil
		},
		retry.Attempts(5),
		retry.Delay(5*time.Second),
	)

	if err != nil {
		utils.Error(err)
		return err
	}

	return sendDataToStore(rabbit, result, reference)
}

func sendDataToStore(rabbit *amqp.Connection, result models.UKAPIPOSTResult, reference string) error {

	message := models.StoreDataMessage{Reference: reference, Result: result}
	body, err := json.Marshal(message)
	if err != nil {
		utils.Error(err)
		return err
	}

	ch, err := rabbit.Channel()
	if err != nil {
		utils.Error(err)
		return err
	}

	q, err := ch.QueueDeclare(os.Getenv("RABBIT_STORE_DATA_QUEUE"), false, false, false, false, nil)
	if err != nil {
		utils.Error(err)
		return err
	}

	err = ch.Publish("", q.Name, false, false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		})

	if err != nil {
		utils.Error(err)
		return err
	}
	return nil
}

func stringCoordsToQueryStruct(coords [][]string) models.UKAPIBulkQuery {
	geolocs := []models.UKAPICoordinate{}

	for _, values := range coords {
		if len(values) != 2 {
			continue
		}
		lat, err := strconv.ParseFloat(values[0], 64)
		if err != nil {
			continue
		}
		lon, err := strconv.ParseFloat(values[1], 64)
		if err != nil {
			continue
		}
		geolocs = append(geolocs, models.UKAPICoordinate{Longitude: lon, Latitude: lat, Radius: 50, Limit: 1})
	}

	return models.UKAPIBulkQuery{geolocs}
}
