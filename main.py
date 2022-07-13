import csv

from sqlalchemy import create_engine, Column, Float, String, MetaData, desc
from sqlalchemy.exc import IntegrityError, StatementError, ArgumentError
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.sql import text

engine = create_engine('sqlite:///investor_db', echo=False)
meta = MetaData()
Base = declarative_base()

Session = sessionmaker(bind=engine)


class Companies(Base):
    __tablename__ = "companies"

    ticker = Column(String, primary_key=True)
    name = Column(String)
    sector = Column(String)

    def __str__(self):
        return f"Company(ticker={self.ticker}, name={self.name}, sector={self.sector})"


class Financial(Base):
    __tablename__ = "financial"

    ticker = Column(String, primary_key=True)
    ebitda = Column(Float, nullable=True, default=None)
    sales = Column(Float, nullable=True, default=None)
    net_profit = Column(Float, nullable=True, default=None)
    market_price = Column(Float, nullable=True, default=None)
    net_debt = Column(Float, nullable=True, default=None)
    assets = Column(Float, nullable=True, default=None)
    equity = Column(Float, nullable=True, default=None)
    cash_equivalents = Column(Float, nullable=True, default=None)
    liabilities = Column(Float, nullable=True, default=None)

    def __str__(self):
        return f"Financial(ticket={self.ticker}, ebitda={self.ebitda}, " \
               f"sales={self.sales}, net_profit={self.net_profit}), " \
               f"market_price={self.market_price}, net_debt={self.net_debt}, " \
               f"assets={self.assets}, equity={self.equity}, cash_equivalents={self.cash_equivalents}, " \
               f"liabilities={self.liabilities})"


def floatDivision(a: float | int | None, b: float | int | None):
    if not a or not b:
        return None
    else:
        return round(a / b, 2)


def crudMenu(current_session):
    print("CRUD MENU\n0 Back\n1 Create a company\n"
          "2 Read a company\n3 Update a company\n"
          "4 Delete a company\n5 List all companies\n")
    try:
        crud_value = int(input("Enter an option:\n"))
        if crud_value == 0:
            print()
        elif crud_value == 1:
            ticker = input("Enter ticker (in the format 'MOON'):\n")
            company = input("Enter company (in the format 'Moon Corp'):\n")
            industries = input("Enter industries (in the format 'Technology'):\n")
            ebitda = input("Enter ebitda (in the format '987654321'):\n")
            sales = int(input("Enter sales (in the format '987654321'):\n"))
            net_profit = int(input("Enter net profit (in the format '987654321'):\n"))
            market_price = int(input("Enter market price (in the format '987654321'):\n"))
            net_debt = int(input("Enter net dept (in the format '987654321'):\n"))
            assets = int(input("Enter assets (in the format '987654321'):\n"))
            equity = int(input("Enter equity (in the format '987654321'):\n"))
            cash_equivalents = int(input("Enter cash equivalents (in the format '987654321'):\n"))
            liabilities = int(input("Enter liabilities (in the format '987654321'):\n"))

            current_session.add(Companies(
                ticker=ticker,
                name=company,
                sector=industries
            ))

            current_session.add(Financial(
                ticker=ticker,
                ebitda=ebitda,
                sales=sales,
                net_profit=net_profit,
                market_price=market_price,
                net_debt=net_debt,
                assets=assets,
                equity=equity,
                cash_equivalents=cash_equivalents,
                liabilities=liabilities
            ))

            current_session.commit()

            print("Company created successfully!\n")
        elif crud_value == 2:
            company = input("Enter company name:\n")
            qc = [c for c in current_session.query(Companies).filter(Companies.name.contains(f"%{company}%")).all()]

            if qc:
                print(*[f"{i} {c.name}" for (i, c) in enumerate(qc)], sep="\n")
                company_n = int(input("Enter company number:\n"))

                queried_company = current_session.query(Financial).filter(Financial.ticker == qc[company_n].ticker).first()

                print(queried_company.ticker, qc[company_n].name)
                print("P/E =", floatDivision(queried_company.market_price, queried_company.net_profit))
                print("P/S =", floatDivision(queried_company.market_price, queried_company.sales))
                print("P/B =", floatDivision(queried_company.market_price, queried_company.assets))
                print("ND/EBITDA =", floatDivision(queried_company.net_debt, queried_company.ebitda))
                print("ROE =", floatDivision(queried_company.net_profit, queried_company.equity))
                print("ROA =", floatDivision(queried_company.net_profit, queried_company.assets))
                print("L/A =", floatDivision(queried_company.liabilities, queried_company.assets))
                print("\n")

            else:
                print("Company not found!")

        elif crud_value == 3:
            company = input("Enter company name:\n")
            qc = [c for c in current_session.query(Companies).filter(Companies.name.contains(f"%{company}%")).all()]

            if qc:
                print(*[f"{i} {c.name}" for (i, c) in enumerate(qc)], sep="\n")
                company_n = int(input("Enter company number:\n"))

                queried_company = current_session.query(Financial).filter(Financial.ticker == qc[company_n].ticker)

                ebitda = input("Enter ebitda (in the format '987654321'):\n")
                sales = int(input("Enter sales (in the format '987654321'):\n"))
                net_profit = int(input("Enter net profit (in the format '987654321'):\n"))
                market_price = int(input("Enter market price (in the format '987654321'):\n"))
                net_debt = int(input("Enter net dept (in the format '987654321'):\n"))
                assets = int(input("Enter assets (in the format '987654321'):\n"))
                equity = int(input("Enter equity (in the format '987654321'):\n"))
                cash_equivalents = int(input("Enter cash equivalents (in the format '987654321'):\n"))
                liabilities = int(input("Enter liabilities (in the format '987654321'):\n"))

                queried_company.update({
                    "ebitda": ebitda,
                    "sales": sales,
                    "net_profit": net_profit,
                    "market_price": market_price,
                    "net_debt": net_debt,
                    "assets": assets,
                    "equity": equity,
                    "cash_equivalents": cash_equivalents,
                    "liabilities": liabilities
                })

                current_session.commit()
                print("Company updated successfully!")
            else:
                print("Company not found!")

        elif crud_value == 4:
            company = input("Enter company name:\n").strip(" ")
            qc = [c for c in current_session.query(Companies).filter(Companies.name.contains(f"%{company}%")).all() or []]

            if qc:
                print(*[f"{i} {c.name}" for (i, c) in enumerate(qc)], sep="\n")
                company_n = int(input("Enter company number:\n"))

                queried_company = current_session.query(Financial).filter(Financial.ticker == qc[company_n].ticker)

                queried_company.delete()
                current_session.commit()

                print("Company deleted successfully!")
            else:
                print("Company not found!")
        elif crud_value == 5:
            print("COMPANY LIST")
            qc = current_session.query(Companies).order_by(Companies.ticker)

            for c in qc:
                print(c.ticker, c.name, c.sector)
        else:
            print("Invalid option!")
    except (ValueError, TypeError):
        print("Invalid option!")


def topTenMenu(current_session):
    print("TOP TEN MENU\n0 Back\n1 List by ND/EBITDA\n2 List by ROE\n3 List by ROA\n")
    top_ten_menu_val = int(input("Enter an option:\n"))
    if top_ten_menu_val == 0:
        print()
    elif top_ten_menu_val == 1:
        print('TICKER N/D EBITDA')
        for ticker, net_debt, ebitda in current_session\
                .query(Financial.ticker, Financial.net_debt, Financial.ebitda) \
                .filter().order_by(desc(Financial.net_debt / Financial.ebitda)) \
                .limit(10):
            print(ticker, floatDivision(net_debt, ebitda))
    elif top_ten_menu_val == 2:
        print('TICKER ROE')
        for ticker, net_profit, equity in current_session \
                .query(Financial.ticker, Financial.net_profit, Financial.equity) \
                .filter().order_by(desc(Financial.net_profit / Financial.equity)) \
                .limit(10):
            print(ticker, floatDivision(net_profit, equity))
    elif top_ten_menu_val == 3:
        print('TICKER ROA')
        for ticker, net_profit, assets in session\
                .execute(text('SELECT ticker, net_profit, assets FROM financial ORDER BY round(net_profit/assets, '
                              '2) desc limit 10;')).fetchall():
            print(ticker, floatDivision(net_profit, assets))
    else:
        print("Invalid option!\n")


def proceed(current_session):
    print("Welcome to the Investor Program!\n")
    while True:
        print("MAIN MENU\n0 Exit\n1 CRUD operations\n2 Show top ten companies by criteria\n")

        try:
            value = int(input("Enter an option:\n"))
            if value == 0:
                break
            elif value == 1:
                crudMenu(current_session)
            elif value == 2:
                topTenMenu(current_session)
            else:
                print("Invalid option!\n")
        except (ValueError, TypeError) as er:
            print(er)
            print("Invalid option!\n")

    print("Have a nice day!")


if __name__ == '__main__':
    Base.metadata.create_all(engine)
    session = Session()

    with open('./test/companies.csv', newline='') as companies, open('./test/financial.csv', newline='') as financial:
        reader = csv.DictReader(companies, delimiter=',')
        for line in reader:
            try:
                session.add(Companies(**line))
                session.flush()
            except (IntegrityError, StatementError, ArgumentError, ValueError, TypeError):
                session.rollback()
        reader = csv.DictReader(financial, delimiter=',')
        for line in reader:
            try:
                session.add(Financial(
                    ticker=line["ticker"],
                    ebitda=line.get("ebitda") or None,
                    sales=line.get("sales") or None,
                    net_profit=line.get("net_profit") or None,
                    market_price=line.get("market_price") or None,
                    net_debt=line.get("net_debt") or None,
                    assets=line.get("assets") or None,
                    equity=line.get("equity") or None,
                    cash_equivalents=line.get("cash_equivalents") or None,
                    liabilities=line.get("liabilities") or None
                ))
                session.flush()

            except (IntegrityError, StatementError, ArgumentError, ValueError, TypeError):
                session.rollback()

        session.commit()

    proceed(session)
    session.close()
