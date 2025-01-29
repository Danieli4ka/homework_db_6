import json
import sqlalchemy
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from models_hw_db import Publisher, Shop, Book, Stock, Sale, create_tables


DB_USER = 'postgres'
DB_PASSWORD = 'postgres'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'netology_db' 

DSN = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
engine = sqlalchemy.create_engine(DSN)
create_tables(engine)

# сессия
Session = sessionmaker(bind=engine)
session = Session()

#json файл
with open('tests_data.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

#загружаем данные в таблицу
model_mapping = {
    'publisher': Publisher,
    'shop': Shop,
    'book': Book,
    'stock': Stock,
    'sale': Sale,
}
with Session() as session:
    try:
        for record in data:
            model = model_mapping.get(record.get('model'))
            if model:
                session.add(model(id=record['pk'], **record['fields']))
        session.commit()
        print('Данные успешно загружены!')
    except Exception as e:
        session.rollback()
        print(f'Ошибка при загрузке данных: {e}')


def get_sales_by_publisher(publisher_input):
    with Session() as session:
        query = (
            session.query(Book.title, Shop.name, Sale.price, Sale.date_sale)
            .join(Publisher, Publisher.id == Book.publisher_id)
            .join(Stock, Stock.book_id == Book.id)
            .join(Shop, Shop.id == Stock.shop_id)
            .join(Sale, Sale.stock_id == Stock.id)
        )

        if publisher_input.isdigit():
            query = query.filter(Publisher.id == int(publisher_input))
        else:
            query = query.filter(Publisher.name == publisher_input)

        results = query.all()

        if results:
            print('Название книги | Магазин | Цена | Дата продажи')
            for title, shop_name, price, date_sale in results:
                print(f'{title} | {shop_name} | {price} | {date_sale}')
        else:
            print('Нет данных по данному издателю.')


if __name__ == "__main__":
    publisher_input = input("Введите имя или ID издателя: ").strip()
    get_sales_by_publisher(publisher_input)
