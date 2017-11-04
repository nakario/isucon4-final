package main

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"log"
	_ "net/http/pprof"

	"github.com/go-redis/redis"
	"github.com/gorilla/mux"
	render2 "github.com/unrolled/render"
	"io/ioutil"
)

type Ad struct {
	Slot        string `json:"slot"`
	Id          string `json:"id"`
	Title       string `json:"title"`
	Type        string `json:"type"`
	Advertiser  string `json:"advertiser"`
	Destination string `json:"destination"`
	Impressions int    `json:"impressions"`
}

type AdWithEndpoints struct {
	Ad
	Asset    string `json:"asset"`
	Redirect string `json:"redirect"`
	Counter  string `json:"counter"`
}

type ClickLog struct {
	AdId   string `json:"ad_id"`
	User   string `json:"user"`
	Agent  string `json:"agent"`
	Gender string `json:"gender"`
	Age    int    `json:"age"`
}

type Report struct {
	Ad          *Ad              `json:"ad"`
	Clicks      int              `json:"clicks"`
	Impressions int              `json:"impressions"`
	Breakdown   *BreakdownReport `json:"breakdown,omitempty"`
}

type BreakdownReport struct {
	Gender      map[string]int `json:"gender"`
	Agents      map[string]int `json:"agents"`
	Generations map[string]int `json:"generations"`
}

type writereq struct {
	id string
	str string
}

type getreq struct {
	str string
	ch chan map[string][]ClickLog
}

var (
	rd *redis.Client
	re = regexp.MustCompile("^bytes=(\\d*)-(\\d*)$")
	r = render2.New()
	OK = []byte("OK")
	writelog = make(chan writereq, 1000)
	reqlog = make(chan getreq, 1000)
)

func init() {
	rd = redis.NewClient(&redis.Options{
		Addr: "webapp3:6379",
		DB:   0,
	})
}

func getDir(name string) string {
	base_dir := "/tmp/go/"
	path := base_dir + name
	os.MkdirAll(path, 0755)
	return path
}

func urlFor(req *http.Request, path string) string {
	host := req.Host
	if host != "" {
		return "http://" + host + path
	} else {
		return path
	}
}

func fetch(hash map[string]string, key string, defaultValue string) string {
	if hash[key] == "" {
		return defaultValue
	} else {
		return hash[key]
	}
}

func incr_map(dict *map[string]int, key string) {
	_, exists := (*dict)[key]
	if !exists {
		(*dict)[key] = 0
	}
	(*dict)[key]++
}

func advertiserId(req *http.Request) string {
	return req.Header.Get("X-Advertiser-Id")
}

func adKey(slot string, id string) string {
	return "isu4:ad:" + slot + "-" + id
}

func assetKey(slot string, id string) string {
	return "isu4:asset:" + slot + "-" + id
}

func advertiserKey(id string) string {
	return "isu4:advertiser:" + id
}

func slotKey(slot string) string {
	return "isu4:slot:" + slot
}

func nextAdId() string {
	id, _ := rd.Incr("isu4:ad-next").Result()
	return strconv.FormatInt(id, 10)
}

func nextAd(req *http.Request, slot string) *AdWithEndpoints {
	key := slotKey(slot)
	id, _ := rd.RPopLPush(key, key).Result()
	if id == "" {
		return nil
	}
	ad := getAd(req, slot, id)
	if ad != nil {
		return ad
	} else {
		rd.LRem(key, 0, id).Result()
		return nextAd(req, slot)
	}
}

func getAd(req *http.Request, slot string, id string) *AdWithEndpoints {
	key := adKey(slot, id)
	m, _ := rd.HGetAll(key).Result()

	if m == nil {
		return nil
	}
	if _, exists := m["id"]; !exists {
		return nil
	}

	imp, _ := strconv.Atoi(m["impressions"])
	path_base := "/slots/" + slot + "/ads/" + id
	var ad *AdWithEndpoints
	ad = &AdWithEndpoints{
		Ad{
			m["slot"],
			m["id"],
			m["title"],
			m["type"],
			m["advertiser"],
			m["destination"],
			imp,
		},
		urlFor(req, path_base+"/asset"),
		urlFor(req, path_base+"/redirect"),
		urlFor(req, path_base+"/count"),
	}
	return ad
}

func decodeUserKey(id string) (string, int) {
	if id == "" {
		return "unknown", -1
	}
	splitted := strings.Split(id, "/")
	gender := "male"
	if splitted[0] == "0" {
		gender = "female"
	}
	age, _ := strconv.Atoi(splitted[1])

	return gender, age
}

func getLogPath(advrId string) string {
	dir := getDir("log")
	splitted := strings.Split(advrId, "/")
	return dir + "/" + splitted[len(splitted)-1]
}

func routePostAd(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	slot := params["slot"]

	advrId := advertiserId(req)
	if advrId == "" {
		log.Println("routePostAdd 404")
		log.Println("slot: " + slot)
		w.WriteHeader(404)
		return
	}

	req.ParseMultipartForm(100 << 20)
	asset := req.MultipartForm.File["asset"][0]
	log.Println("asset.Size:", asset.Size)
	id := nextAdId()
	key := adKey(slot, id)

	content_type := ""
	if len(req.Form["type"]) > 0 {
		content_type = req.Form["type"][0]
	}
	if content_type == "" && len(asset.Header["Content-Type"]) > 0 {
		content_type = asset.Header["Content-Type"][0]
	}
	if content_type == "" {
		content_type = "video/mp4"
	}

	title := ""
	if a := req.Form["title"]; a != nil {
		title = a[0]
	}
	destination := ""
	if a := req.Form["destination"]; a != nil {
		destination = a[0]
	}

	rd.HMSet(key, map[string]interface{}{
		"slot": slot,
		"id": id,
		"title": title,
		"type": content_type,
		"advertiser": advrId,
		"destination": destination,
		"impressions": "0",
	})

	f, _ := asset.Open()
	defer f.Close()
	os.MkdirAll("/home/isucon/assets/" + slot, 0777)
	out, _ := os.Create("/home/isucon/assets/" + slot + "/" + id)
	defer out.Close()
	io.Copy(out, f)

	rd.RPush(slotKey(slot), id)
	rd.SAdd(advertiserKey(advrId), key)

	r.JSON(w,200, getAd(req, slot, id))
}

func routeGetAd(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	slot := params["slot"]
	ad := nextAd(req, slot)
	if ad != nil {
		http.Redirect(w, req, "/slots/" + slot + "/ads/" + ad.Id, http.StatusFound)
	} else {
		log.Println("routeGetAd 404")
		log.Println("slot: " + slot)
		r.JSON(w,404, map[string]string{"error": "not_found"})
	}
}

func routeGetAdWithId(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	slot := params["slot"]
	id := params["id"]
	ad := getAd(req, slot, id)
	if ad != nil {
		r.JSON(w,200, ad)
	} else {
		log.Println("routeGetAdWithId 404")
		log.Println("slot/id: " + slot + "/" + id)
		r.JSON(w,404, map[string]string{"error": "not_found"})
	}
}

func routeGetAdAsset(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	slot := params["slot"]
	id := params["id"]
	ad := getAd(req, slot, id)
	if ad == nil {
		log.Println("routeGetAdAsset 404")
		log.Println("slot/id: " + slot + "/" + id)
		r.JSON(w,404, map[string]string{"error": "not_found"})
		return
	}
	content_type := "application/octet-stream"
	if ad.Type != "" {
		content_type = ad.Type
	}

	w.Header().Set("Content-Type", content_type)
	data, err := ioutil.ReadFile("/home/isucon/assets/" + slot + "/" + id)
	if os.IsNotExist(err) {
		log.Println("Redirect from " + req.Host + " to another")
		if req.Host == "webapp1" {
			http.Redirect(w, req, "webapp2/slots/" + slot + "/ads/" + id + "/asset", http.StatusMovedPermanently)
			return
		} else if req.Host == "webapp2" {
			http.Redirect(w, req, "webapp2/slots/" + slot + "/ads/" + id + "/asset", http.StatusMovedPermanently)
			return
		} else {
			log.Println("routeGetAdAsset 404")
			log.Println("slot/id: " + slot + "/" + id)
			r.JSON(w,404, map[string]string{"error": "not_found"})
			return
		}
	}

	range_str := req.Header.Get("Range")
	if range_str == "" {
		r.Data(w,200, data)
		return
	}

	m := re.FindAllStringSubmatch(range_str, -1)

	if m == nil {
		w.WriteHeader(416)
		return
	}

	head_str := m[0][1]
	tail_str := m[0][2]

	if head_str == "" && tail_str == "" {
		w.WriteHeader(416)
		return
	}

	head := 0
	tail := 0

	if head_str != "" {
		head, _ = strconv.Atoi(head_str)
	}
	if tail_str != "" {
		tail, _ = strconv.Atoi(tail_str)
	} else {
		tail = len(data) - 1
	}

	if head < 0 || head >= len(data) || tail < 0 {
		w.WriteHeader(416)
		return
	}

	range_data := data[head:(tail + 1)]
	content_range := fmt.Sprintf("bytes %d-%d/%d", head, tail, len(data))
	w.Header().Set("Content-Range", content_range)
	w.Header().Set("Content-Length", strconv.Itoa(len(range_data)))

	r.Data(w, 206, range_data)
}

func routeGetAdCount(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	slot := params["slot"]
	id := params["id"]
	key := adKey(slot, id)

	_, err := rd.Exists(key).Result()
	if err == redis.Nil {
		log.Println("routeGetAdCount 404")
		log.Println("slot/id: " + slot + "/" + id)
		r.JSON(w, 404, map[string]string{"error": "not_found"})
		return
	}

	rd.HIncrBy(key, "impressions", 1).Result()
	w.WriteHeader(204)
}

func routeGetAdRedirect(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	slot := params["slot"]
	id := params["id"]
	ad := getAd(req, slot, id)

	if ad == nil {
		log.Println("routeGetAdRedirect 404")
		log.Println("slot/id: " + slot + "/" + id)
		r.JSON(w, 404, map[string]string{"error": "not_found"})
		return
	}

	isuad := ""
	cookie, err := req.Cookie("isuad")
	if err != nil {
		if err != http.ErrNoCookie {
			panic(err)
		}
	} else {
		isuad = cookie.Value
	}
	ua := req.Header.Get("User-Agent")

	writelog <- writereq{ad.Advertiser, fmt.Sprintf("%s\t%s\t%s\n", ad.Id, isuad, ua)}

	http.Redirect(w, req, ad.Destination, http.StatusFound)
}

func routeGetReport(w http.ResponseWriter, req *http.Request) {
	advrId := advertiserId(req)

	if advrId == "" {
		w.WriteHeader(401)
		return
	}

	report := map[string]*Report{}
	adKeys, _ := rd.SMembers(advertiserKey(advrId)).Result()
	for _, adKey := range adKeys {
		ad, _ := rd.HGetAll(adKey).Result()
		if ad == nil {
			continue
		}

		imp, _ := strconv.Atoi(ad["impressions"])
		data := &Report{
			&Ad{
				ad["slot"],
				ad["id"],
				ad["title"],
				ad["type"],
				ad["advertiser"],
				ad["destination"],
				imp,
			},
			0,
			imp,
			nil,
		}
		report[ad["id"]] = data
	}

	getlog := make(chan map[string][]ClickLog, 1)
	reqlog <- getreq{advrId, getlog}
	logs := <- getlog
	for adId, clicks := range logs {
		if _, exists := report[adId]; !exists {
			report[adId] = &Report{}
		}
		report[adId].Clicks = len(clicks)
	}
	r.JSON(w, 200, report)
}

func routeGetFinalReport(w http.ResponseWriter, req *http.Request) {
	advrId := advertiserId(req)

	if advrId == "" {
		w.WriteHeader(401)
		return
	}

	reports := map[string]*Report{}
	adKeys, _ := rd.SMembers(advertiserKey(advrId)).Result()
	for _, adKey := range adKeys {
		ad, _ := rd.HGetAll(adKey).Result()
		if ad == nil {
			continue
		}

		imp, _ := strconv.Atoi(ad["impressions"])
		data := &Report{
			&Ad{
				ad["slot"],
				ad["id"],
				ad["title"],
				ad["type"],
				ad["advertiser"],
				ad["destination"],
				imp,
			},
			0,
			imp,
			nil,
		}
		reports[ad["id"]] = data
	}

	getlog := make(chan map[string][]ClickLog, 1)
	reqlog <- getreq{advrId, getlog}
	logs := <- getlog

	for adId, report := range reports {
		log, exists := logs[adId]
		if exists {
			report.Clicks = len(log)
		}

		breakdown := &BreakdownReport{
			map[string]int{},
			map[string]int{},
			map[string]int{},
		}
		for i := range log {
			click := log[i]
			incr_map(&breakdown.Gender, click.Gender)
			incr_map(&breakdown.Agents, click.Agent)
			generation := "unknown"
			if click.Age != -1 {
				generation = strconv.Itoa(click.Age / 10)
			}
			incr_map(&breakdown.Generations, generation)
		}
		report.Breakdown = breakdown
		reports[adId] = report
	}

	r.JSON(w, 200, reports)
}

func routePostInitialize(w http.ResponseWriter, req *http.Request) {
	keys, _ := rd.Keys("isu4:*").Result()
	for i := range keys {
		key := keys[i]
		rd.Del(key)
	}
	path := getDir("log")
	os.RemoveAll(path)
	os.RemoveAll("/home/isucon/assets/")
	os.MkdirAll("/home/isucon/assets/", 0777)

	w.WriteHeader(200)
	w.Write(OK)
}

func loghandler() {
	for {
		select {
			case req := <- reqlog:

				path := getLogPath(req.str)
				result := map[string][]ClickLog{}
				if _, err := os.Stat(path); os.IsNotExist(err) {
					req.ch <- result
					continue
				}

				f, err := os.Open(path)
				if err != nil {
					panic(err)
				}
				defer f.Close()

				scanner := bufio.NewScanner(f)
				for scanner.Scan() {
					line := scanner.Text()
					line = strings.TrimRight(line, "\n")
					sp := strings.Split(line, "\t")
					ad_id := sp[0]
					user := sp[1]
					agent := sp[2]
					if agent == "" {
						agent = "unknown"
					}
					gender, age := decodeUserKey(sp[1])
					if result[ad_id] == nil {
						result[ad_id] = []ClickLog{}
					}
					data := ClickLog{ad_id, user, agent, gender, age}
					result[ad_id] = append(result[ad_id], data)
				}

				req.ch <- result

			case req := <- writelog:
				path := getLogPath(req.id)

				var f *os.File
				f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
				if err != nil {
					panic(err)
				}

				fmt.Fprint(f, req.str)
				f.Close()
		}
	}
}

func main() {
	go loghandler()

	router := mux.NewRouter()

	slots := router.PathPrefix("/slots/{slot}").Subrouter()
	slots.HandleFunc("/ads", routePostAd).Methods("POST")
	slots.HandleFunc("/ad", routeGetAd).Methods("GET")
	slots.HandleFunc("/ads/{id}", routeGetAdWithId).Methods("GET")
	slots.HandleFunc("/ads/{id}/asset", routeGetAdAsset).Methods("GET")
	slots.HandleFunc("/ads/{id}/count", routeGetAdCount).Methods("POST")
	slots.HandleFunc("/ads/{id}/redirect", routeGetAdRedirect).Methods("GET")

	me := router.PathPrefix("/me").Subrouter()
	me.HandleFunc("/report", routeGetReport).Methods("GET")
	me.HandleFunc("/final_report", routeGetFinalReport).Methods("GET")

	router.HandleFunc("/initialize", routePostInitialize).Methods("POST")
	router.PathPrefix("/").Handler(http.FileServer(http.Dir("../public")))
	router.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		log.Println("not found")
		log.Println("req; ", *req)
		http.NotFound(w, req)
	})
	http.Handle("/", router)
	http.ListenAndServe(":8080", nil)
}
