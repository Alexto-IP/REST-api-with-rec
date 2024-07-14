from fastapi import FastAPI, Response, status
from pydantic import BaseModel
import sqlite3

app = FastAPI()


class SQLigther:

    def __init__(self, database):
        """Подключаемся к БД и сохраняем курсор соединения"""
        self.connection = sqlite3.connect(database, check_same_thread=False)
        self.cursor = self.connection.cursor()
        self.is_processing = False

    def get_all_items_id(self):
        """Получаем id всех продуктов"""
        with self.connection:
            return self.cursor.execute('SELECT `item_id` FROM `catalog`').fetchall()

    def get_item_name(self, item_id):
        """Получаем название продукта по id"""
        with self.connection:
            return self.cursor.execute('SELECT `item_name` FROM `catalog` WHERE `item_id` = ?', (item_id,)).fetchone()[0]

    def get_item_type(self, item_id):
        """Получаем категорию продукта по id"""
        with self.connection:
            type_id = self.cursor.execute('SELECT `item_type` FROM `catalog` WHERE `item_id` = ?', (item_id,)).fetchone()[0]
            return self.cursor.execute('SELECT `type_name` FROM `types` WHERE `type_id` = ?', (type_id,)).fetchone()[0]

    def get_item_price(self, item_id):
        """Получаем цену продукта по id"""
        with self.connection:
            return self.cursor.execute('SELECT `item_price` FROM `catalog` WHERE `item_id` = ?', (item_id,)).fetchone()[0]

    def get_item_count(self, item_id):
        """Получаем количество продукта по id"""
        with self.connection:
            return self.cursor.execute('SELECT `item_count` FROM `catalog` WHERE `item_id` = ?', (item_id,)).fetchone()[0]

    def get_item_discount(self, item_id):
        """Получаем количество продукта по id"""
        with self.connection:
            return self.cursor.execute('SELECT `item_discount` FROM `catalog` WHERE `item_id` = ?', (item_id,)).fetchone()[0]

    def get_item_rating(self, item_id):
        """Получаем количество продукта по id"""
        with self.connection:
            return self.cursor.execute('SELECT `item_rating` FROM `catalog` WHERE `item_id` = ?', (item_id,)).fetchone()[0]

    def get_user_preferences(self, user_id):
        """Получаем предпочтения пользователя"""
        with self.connection:
            return self.cursor.execute(
                'SELECT item_type, total_type_amount, average_type_rating, count_of_type_purchased '
                'FROM interests WHERE user_id = ?', (user_id,)
            ).fetchall()

    def get_type(self, type_id):
        """Получаем имя типа по id"""
        with self.connection:
            return self.cursor.execute('SELECT `type_name` FROM `types` WHERE `type_id` = ?', (type_id,)).fetchone()[0]


db = SQLigther('shop.db')


class Item(BaseModel):
    id: int
    name: str
    type: str
    price: int
    count: int
    discount: int
    rating: float


class ItemOnScreen(BaseModel):
    id: int
    name: str
    price: int
    discount: int


def recommend_items(user_id):
    user_preferences = db.get_user_preferences(user_id)
    if not user_preferences:
        return []

    user_preferences = [[db.get_type(pref[0])] + list(pref[1:]) for pref in user_preferences]
    user_preferences_dict = {pref[0]: pref[1:] for pref in user_preferences}

    items_id = db.get_all_items_id()
    recommendations = []

    for item_id in items_id:
        item_id = item_id[0]
        item_type = db.get_item_type(item_id)
        if item_type in user_preferences_dict:
            recommendations.append({
                "id": item_id,
                "name": db.get_item_name(item_id),
                "type": db.get_item_type(item_id),
                "price": db.get_item_price(item_id),
                "count": db.get_item_count(item_id),
                "discount": db.get_item_discount(item_id),
                "rating": db.get_item_rating(item_id)
            })

    recommendations = sorted(recommendations, key=lambda x: (
        user_preferences_dict[x['type']][0],  # total_type_amount
        user_preferences_dict[x['type']][1],  # average_type_rating
        user_preferences_dict[x['type']][2]   # count_of_type_purchased
    ), reverse=True)

    return recommendations


def get_popular_items():
    items_id = db.get_all_items_id()
    popular_items = []

    for item_id in items_id:
        item_id = item_id[0]
        popular_items.append({
            "id": item_id,
            "name": db.get_item_name(item_id),
            "type": db.get_item_type(item_id),
            "price": db.get_item_price(item_id),
            "count": db.get_item_count(item_id),
            "discount": db.get_item_discount(item_id),
            "rating": db.get_item_rating(item_id)
        })

    popular_items = sorted(popular_items, key=lambda x: x['rating'], reverse=True)

    return popular_items[:20]


@app.get('/items/get/all')
def get_items():
    res = []
    items_id = db.get_all_items_id()
    for item in items_id:
        item_id = item[0]
        item_data = {
            "id": item_id,
            "name": db.get_item_name(item_id),
            "price": db.get_item_price(item_id),
            "discount": db.get_item_discount(item_id)
        }
        res.append(ItemOnScreen(**item_data))
    return res


@app.get('/items/get/recommend/all')
def get_recommendations():
    rec_items = get_popular_items()
    return [ItemOnScreen(**item) for item in rec_items]


@app.get('/items/get/recommend/{user_id}')
def get_recommendations(user_id: int):
    rec_items = recommend_items(user_id)
    if len(rec_items) == 0:
        rec_items = get_popular_items()
    return [ItemOnScreen(**item) for item in rec_items]


@app.get('/items/get/{item_id}')
def get_item(item_id: int, response: Response):
    items_id = db.get_all_items_id()
    items_id = [item[0] for item in items_id]
    if item_id in items_id:
        item = {
            "id": item_id,
            "name": db.get_item_name(item_id),
            "type": db.get_item_type(item_id),
            "price": db.get_item_price(item_id),
            "count": db.get_item_count(item_id),
            "discount": db.get_item_discount(item_id),
            "rating": db.get_item_rating(item_id)
        }
        return Item(**item)
    else:
        response.status_code = status.HTTP_404_NOT_FOUND
        return False
