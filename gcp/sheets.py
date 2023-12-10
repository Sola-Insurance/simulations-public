import gspread


gc = gspread.service_account(filename='../files/credentials.json')

sh = gc.open_by_key('1p_6CDHkQ0iRkgu79NYn04Nc1-pe-OaJOJ8_EuXGkrnY')


def get_worksheet(name: str):
    return sh.worksheet(name)
