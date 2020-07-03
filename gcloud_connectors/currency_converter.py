import requests


class ForeignExchangeRatesConverter:
    def get_exchange_by_date(self, date, base_currency='EUR'):
        """
        :param date: date for currency conversion rates in %Y-%m-%d
        :param base_currency: currency in iso format 3 to discover conversions
        :return: json response of currency conversion
        """
        url = "https://api.exchangeratesapi.io/{date}?base={base_currency}" \
            .format(date=date, base_currency=base_currency)
        response = requests.request("GET", url)
        json_response = {}

        if response.ok:
            json_response = response.json().get('rates', {})
            for currency in json_response.keys():
                json_response[currency] = 1 / json_response[currency]

            json_response['base_currency'] = 'EUR'
            json_response['date'] = date

        return json_response
