package main

import (
	"context"
	"fmt"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"math"
	"sort"
	"time"
)

type PriceHistory struct {
	Name     string
	Price    float64
	Link     string
	DateTime time.Time
}

func main() {
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Подключитесь к серверу MongoDB.
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(ctx)

	// Выберите базу данных и коллекцию.
	collection := client.Database("avito").Collection("avito_collection")
	priceHistoryCollection := client.Database("avito").Collection("price_history")

	for {
		checkAndUpdatePrices(ctx, collection, priceHistoryCollection)
		time.Sleep(60 * time.Second)
	}

}
func checkAndUpdatePrices(ctx context.Context, collection *mongo.Collection, priceHistoryCollection *mongo.Collection) {
	// Создайте агрегацию для получения наименьшей цены с учетом условия разницы не более 20% и соответствующей ссылки для первых 10 наиболее часто встречающихся наименований.
	pipeline := []bson.M{
		{
			"$group": bson.M{
				"_id":      "$title",
				"count":    bson.M{"$sum": 1},
				"minPrice": bson.M{"$min": "$price"}, // Получить наименьшую цену в группе.
				"link":     bson.M{"$first": "$url"}, // Получить ссылку первого документа в группе.
			},
		},
		{
			"$match": bson.M{
				"count":    bson.M{"$gt": 3}, // Условие "больше 6 раз".
				"minPrice": bson.M{"$ne": 0}, // Исключаем цены равные 0.
			},
		},
		{
			"$sort": bson.M{
				"count": -1, // Сортировка по убыванию.
			},
		},
		{
			"$limit": 40, // Ограничение на первые 15 результатов.
		},
	}

	cursor, err := collection.Aggregate(ctx, pipeline)
	if err != nil {
		log.Fatal(err)
	}
	defer cursor.Close(ctx)

	// Пройдемся по результатам агрегации и выведем наименьшую цену и соответствующую ссылку для первых 10 наиболее часто встречающихся наименований.
	for cursor.Next(ctx) {
		var result bson.M
		if err := cursor.Decode(&result); err != nil {
			log.Fatal(err)
		}

		name := result["_id"].(string)
		count := result["count"].(int32)

		// Проверка, что найденное наименование не пусто
		if name != "" {
			// Получите все цены и ссылки для данного наименования
			prices, links, err := getPricesAndLinks(ctx, collection, name)
			if err != nil {
				log.Fatal(err)
			}

			// Вызов функции для вычисления наименьшей цены с учетом условия разницы не более 20%
			minValidPrice, link := findMinPriceWithCondition(prices, links)

			// Проверка, что найденная цена не равна максимальной и не равна нулю
			if minValidPrice != math.MaxFloat64 && minValidPrice != 0 {
				message := fmt.Sprintf("Наименование: %s, \nКоличество: %d, \nНаименьшая цена: %.2f, \nСсылка: %s", name, count, minValidPrice, link)

				if prevPriceInfo, ok := getPreviousPriceInfo(ctx, priceHistoryCollection, name); ok {
					// Если цена изменилась более чем на 1%, то обновляем запись
					priceChangeThreshold := 0.01
					if math.Abs(minValidPrice-prevPriceInfo.Price)/prevPriceInfo.Price > priceChangeThreshold {
						// Обновляем существующую запись
						filter := bson.M{"name": name}
						update := bson.M{
							"$set": bson.M{
								"price":    minValidPrice,
								"link":     link,
								"datetime": time.Now(),
							},
						}

						_, err := priceHistoryCollection.UpdateOne(ctx, filter, update)

						if err != nil {
							log.Fatal(err)
						}

						// Отправляем сообщение об обновлении цены
						sendMessageToTelegram("Цена была обновлена: " + message)
					} else {
						// Отправляем сообщение о том, что цена осталась неизменной
						//sendMessageToTelegram("Цена осталась неизменной: " + message)
					}
				} else {
					// Создайте экземпляр PriceHistory
					priceHistory := PriceHistory{
						Name:     name,
						Price:    minValidPrice,
						Link:     link,
						DateTime: time.Now(),
					}

					// Вставьте запись истории цен в коллекцию "price_history"
					_, err := priceHistoryCollection.InsertOne(ctx, priceHistory)

					if err != nil {
						log.Fatal(err)
					}

					// Отправить сообщение о создании новой записи
					sendMessageToTelegram("Цена была обновлена: " + message)
				}

			}
		}
	}
}

func findMinPriceWithCondition(prices []float64, links []string) (float64, string) {
	if len(prices) == 0 {
		return 0, ""
	}

	// Определяем структуру для хранения цены и соответствующей ссылки
	type PriceWithLink struct {
		Price float64
		Link  string
	}

	priceLinks := make([]PriceWithLink, len(prices))

	// Игнорируем цены, равные 1
	validPriceLinks := make([]PriceWithLink, 0)
	for i, price := range prices {
		if price != 1.0 {
			priceLinks[i] = PriceWithLink{Price: price, Link: links[i]}
			validPriceLinks = append(validPriceLinks, priceLinks[i])
		}
	}

	// Сортируем по цене в порядке возрастания
	sort.Slice(validPriceLinks, func(i, j int) bool {
		return validPriceLinks[i].Price < validPriceLinks[j].Price
	})

	// Вычисляем порог процентной разницы (20%)
	priceThreshold := 0.20

	// Создаем срез для хранения элементов, удовлетворяющих условию
	var validPriceLinksCondition []PriceWithLink
	var currentGroup []PriceWithLink
	var groups [][]PriceWithLink

	for i := 1; i < len(validPriceLinks); i++ {
		valid := false
		for j := i - 1; j >= 0; j-- {
			priceDifference := (validPriceLinks[i].Price - validPriceLinks[j].Price) / validPriceLinks[j].Price
			if priceDifference <= priceThreshold {
				valid = true
				break
			}
		}

		if valid {
			currentGroup = append(currentGroup, validPriceLinks[i-1])
		} else {
			// Если разница в цене более 20%, сохраняем текущую группу и начинаем новую
			if len(currentGroup) > 0 {
				groups = append(groups, currentGroup)
				currentGroup = nil
			}
		}
	}

	// Добавляем последнюю группу
	if len(currentGroup) > 0 {
		groups = append(groups, currentGroup)
	}

	// Теперь у нас есть разделенные группы в слайсе "groups".
	// Мы можем определить, в какой из них больше элементов и добавить её в "validPriceLinksCondition".
	maxGroupSize := 0
	maxGroupIndex := -1

	for i, group := range groups {
		if len(group) > maxGroupSize {
			maxGroupSize = len(group)
			maxGroupIndex = i
		}
	}

	if maxGroupIndex >= 0 {
		validPriceLinksCondition = append(validPriceLinksCondition, groups[maxGroupIndex]...)
	}

	// Сортируем validPriceLinksCondition в порядке возрастания
	sort.Slice(validPriceLinksCondition, func(i, j int) bool {
		return validPriceLinksCondition[i].Price < validPriceLinksCondition[j].Price
	})

	// Выводим минимальную цену и соответствующую ссылку
	if len(validPriceLinksCondition) > 0 {
		minValidPrice := validPriceLinksCondition[0].Price
		validLink := validPriceLinksCondition[0].Link
		fmt.Printf("Минимальная цена: %.2f\nСоответствующая ссылка: %s\n", minValidPrice, validLink)
		return minValidPrice, validLink
	}

	return 0, ""
}

// Функция для получения цен и соответствующих ссылок для определенного наименования.
func getPricesAndLinks(ctx context.Context, collection *mongo.Collection, name string) ([]float64, []string, error) {
	filter := bson.M{"title": name}
	cur, err := collection.Find(ctx, filter, options.Find().SetProjection(bson.M{"price": 1, "url": 1}))
	if err != nil {
		return nil, nil, err
	}
	defer cur.Close(ctx)

	var prices []float64
	var links []string

	for cur.Next(ctx) {
		var result bson.M
		if err := cur.Decode(&result); err != nil {
			return nil, nil, err
		}

		price, ok := result["price"].(float64)
		if !ok {
			continue
		}

		link, ok := result["url"].(string)
		if !ok {
			continue
		}

		prices = append(prices, price)
		links = append(links, link)
	}

	return prices, links, nil
}

// Функция для отправки сообщения в Telegram.
func sendMessageToTelegram(message string) {
	bot, err := tgbotapi.NewBotAPI("6588492712:AAHr0tK6WvmrAgHYWYq1oSm4sZ5-yRdX_GU")
	if err != nil {
		log.Fatal(err)
	}

	chatID := int64(1231104328) // Замените на ваш ID чата
	msg := tgbotapi.NewMessage(chatID, message)

	_, err = bot.Send(msg)
	if err != nil {
		log.Fatal(err)
	}
}

// Функция для получения информации о предыдущей цене из коллекции "price_history"
func getPreviousPriceInfo(ctx context.Context, collection *mongo.Collection, name string) (PriceHistory, bool) {
	filter := bson.M{"name": name}
	opts := options.FindOne().SetSort(bson.M{"DateTime": -1})
	var priceHistory PriceHistory
	err := collection.FindOne(ctx, filter, opts).Decode(&priceHistory)
	if err != nil {
		return PriceHistory{}, false
	}
	return priceHistory, true
}
