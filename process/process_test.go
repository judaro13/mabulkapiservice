package process

import (
	"log"
	"os"
	"testing"

	"github.com/dnaeon/go-vcr/recorder"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

func TestData(t *testing.T) {
	r, err := recorder.New("fixtures/process_data")
	if err != nil {
		log.Fatal(err)
	}
	defer r.Stop()

	if len(os.Getenv("RABBIT_URL")) == 0 {
		os.Setenv("RABBIT_URL", "amqp://guest:guest@localhost:5672/")
	}
	conn, err := amqp.Dial(os.Getenv("RABBIT_URL"))
	defer conn.Close()
	assert.NoError(t, err)

	data := []byte("{asdfa}")
	err = Data(conn, data)
	assert.Error(t, err)

	data = []byte("{\"reference\":\"371a23d4-a8a3-4cc4-a01b-6a5c6aa21ca6\",\"coordinates\":[[\"lat\",\"lon\"],[\"52.923454\",\"-1.474217\"],[\"53.457321\",\"-2.262773\"],[\"50.871446\",\"-0.729985\"],[\"50.215687\",\"-5.191573\"],[\"57.540178\",\"-3.758607\"],[\"51.126982\",\"1.323783\"],[\"52.248118\",\"0.109363\"],[\"52.73052\",\"-1.102517\"],[\"51.643843\",\"-0.427707\"],[\"51.543989\",\"-0.214267\"],[\"51.526783\",\"0.598957\"],[\"52.452202\",\"1.439576\"],[\"51.425096\",\"-0.57365\"],[\"51.377773\",\"-2.528614\"],[\"51.745122\",\"-0.450282\"],[\"51.541592\",\"0.043358\"],[\"56.478868\",\"-3.013486\"],[\"53.710412\",\"-2.251032\"],[\"53.376684\",\"-3.182967\"],[\"53.569599\",\"-2.586036\"],[\"51.318225\",\"-2.206578\"],[\"51.664826\",\"0.376871\"],[\"51.797923\",\"-4.968177\"],[\"51.905654\",\"-2.08205\"],[\"51.304335\",\"-0.635272\"],[\"51.401441\",\"-0.765678\"],[\"50.829726\",\"-1.081785\"],[\"51.199023\",\"-2.54258\"],[\"51.611757\",\"0.273606\"],[\"51.391512\",\"-0.107147\"],[\"53.220124\",\"-0.590571\"],[\"53.802373\",\"-2.454281\"],[\"51.550839\",\"-0.1467\"],[\"54.594079\",\"-5.870061\"],[\"51.564173\",\"-3.017777\"],[\"52.656795\",\"-2.038305\"],[\"54.367907\",\"-7.733817\"],[\"51.214771\",\"-1.101123\"],[\"54.687937\",\"-1.224822\"],[\"55.71193\",\"-4.529017\"],[\"53.367907\",\"-2.076966\"],[\"51.436445\",\"-0.053119\"],[\"51.400156\",\"-0.463046\"],[\"55.955099\",\"-3.196319\"],[\"51.433739\",\"-0.456683\"],[\"53.446968\",\"-1.987735\"],[\"51.544722\",\"-0.176583\"],[\"51.291987\",\"-2.441811\"],[\"52.568861\",\"-0.242024\"],[\"55.948809\",\"-4.774202\"],[\"51.984145\",\"-2.41899\"],[\"51.405547\",\"-3.271888\"],[\"51.489388\",\"0.018004\"],[\"51.417427\",\"-0.080494\"],[\"55.161511\",\"-3.717003\"],[\"51.35165\",\"-1.991924\"],[\"53.572172\",\"-0.077361\"],[\"51.40404\",\"-0.173486\"],[\"54.176641\",\"-0.287341\"],[\"53.442701\",\"-2.962761\"],[\"52.513627\",\"-3.310355\"],[\"54.016396\",\"-1.076919\"],[\"55.733182\",\"-3.962703\"],[\"51.543815\",\"0.199015\"]]}")
	err = Data(conn, data)

	assert.NoError(t, err)
}

func TestStringCoordsToQueryStruct(t *testing.T) {
	coord := [][]string{[]string{"51.543989", "-0.214267"},
		[]string{"51.526783", "0.598957"},
		[]string{"52.452202", "1.439576"},
		[]string{"51.425096", "-0.57365"}}

	result := stringCoordsToQueryStruct(coord)
	assert.Equal(t, len(coord), len(result.Geolocations))
}
