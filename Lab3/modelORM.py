import sys

from sqlalchemy import create_engine, ForeignKey
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker, relationship

Base = declarative_base()

class Customer(Base):
    __tablename__ = 'customer'
    Customer_ID = Column(Integer, primary_key = True)
    Name = Column(String)

class Order(Base):
    __tablename__ = 'order'
    Order_ID = Column(Integer, primary_key = True)
    _Customer = Column(String, ForeignKey('customer.name'))

class Products(Base):
    __tablename__ = 'products'
    Product_ID = Column(Integer, primary_key = True)
    Price = Column(Integer)


class Products_order(Base):
    __tablename__ = 'products_order'
    _order = Column(Integer, primary_key = True)
    _product = Column(Integer)
    quantity = Column(Integer)

DATABASE_URI = 'postgres+psycopg2://postgres:VI@localhost:5432/postgres'
engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)
s = Session()

def insert_customer(values: tuple):
    try:
        customer = Customer(Customer_ID = values[0], Name =values[1])
        s.add(customer)
        s.commit()
    except SQLAlchemyError as e:
        print(e)

def insert_order(values: tuple):
    try:
        order = Order(Order_ID=values[0], _Customer=values[1])
        s.add(order)
        s.commit()
    except SQLAlchemyError as e:
        print(e)

def insert_products(values: tuple):
    try:
        products = Products(Product_ID = values[0], Price=values[1])
        s.add(products)
        s.commit()
    except SQLAlchemyError as e:
        print(e)

def insert_products_order(values: tuple):
    try:
        products_order = Products_order(_order=values[0], _product=values[1], quantity = values[2])
        s.add(products_order)
        s.commit()
    except SQLAlchemyError as e:
        print(e)


def delete(t_name, column, value):
    try:
        t_class = getattr(sys.modules[__name__], t_name.capitalize())
        t_class_col = getattr(t_class, column)
        s.query(t_class).filter(t_class_col == value).delete()
        s.commit()
    except SQLAlchemyError as e:
        print(e)


def update(t_name, column, value, cond):
    try:
        t_class = getattr(sys.modules[__name__], t_name.capitalize())
        t_class_col = getattr(t_class, column)
        t_class_cond_col = getattr(t_class, cond[0])
        s.query(t_class).filter(t_class_cond_col == cond[1]).update({t_class_col: value})
        s.commit()
    except SQLAlchemyError as e:
        print(e)