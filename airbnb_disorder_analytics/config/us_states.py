'''
us_states.py
allow users to convert from state abbreviations to full names in the specified format

ruilin chen
08/09/2020
'''

__all__ = ['USStates']


class USStates:
    def __init__(self):
        self.abbr_to_name = {
            "AL": "Alabama",
            "AK": "Alaska",
            "AZ": "Arizona",
            "AR": "Arkansas",
            "CA": "California",
            "CO": "Colorado",
            "CT": "Connecticut",
            "DE": "Delaware",
            "DC": "District of Columbia",
            "FL": "Florida",
            "GA": "Georgia",
            "HI": "Hawaii",
            "ID": "Idaho",
            "IL": "Illinois",
            "IN": "Indiana",
            "IA": "Iowa",
            "KS": "Kansas",
            "KY": "Kentucky",
            "LA": "Louisiana",
            "ME": "Maine",
            "MD": "Maryland",
            "MA": "Massachusetts",
            "MI": "Michigan",
            "MN": "Minnesota",
            "MS": "Mississippi",
            "MO": "Missouri",
            "MT": "Montana",
            "NE": "Nebraska",
            "NV": "Nevada",
            "NH": "New Hampshire",
            "NJ": "New Jersey",
            "NM": "New Mexico",
            "NY": "New York",
            "NC": "North Carolina",
            "ND": "North Dakota",
            "OH": "Ohio",
            "OK": "Oklahoma",
            "OR": "Oregon",
            "PA": "Pennsylvania",
            "RI": "Rhode Island",
            "SC": "South Carolina",
            "SD": "South Dakota",
            "TN": "Tennessee",
            "TX": "Texas",
            "UT": "Utah",
            "VT": "Vermont",
            "VA": "Virginia",
            "WA": "Washington",
            "WV": "West Virginia",
            "WI": "Wisconsin",
            "WY": "Wyoming"
        }

        self.fips_to_abbr = { '01': 'AL',
                              '02': 'AK',
                              '04': 'AZ',
                              '05': 'AR',
                              '06': 'CA',
                              '08': 'CO',
                              '09': 'CT',
                              '10': 'DE',
                              '11': 'DC',
                              '12': 'FL',
                              '13': 'GA',
                              '15': 'HI',
                              '16': 'ID',
                              '17': 'IL',
                              '18': 'IN',
                              '19': 'IA',
                              '20': 'KS',
                              '21': 'KY',
                              '22': 'LA',
                              '23': 'ME',
                              '24': 'MD',
                              '25': 'MA',
                              '26': 'MI',
                              '27': 'MN',
                              '28': 'MS',
                              '29': 'MO',
                              '30': 'MT',
                              '31': 'NE',
                              '32': 'NV',
                              '33': 'NH',
                              '34': 'NJ',
                              '35': 'NM',
                              '36': 'NY',
                              '37': 'NC',
                              '38': 'ND',
                              '39': 'OH',
                              '40': 'OK',
                              '41': 'OR',
                              '42': 'PA',
                              '44': 'RI',
                              '45': 'SC',
                              '46': 'SD',
                              '47': 'TN',
                              '48': 'TX',
                              '49': 'UT',
                              '50': 'VT',
                              '51': 'VA',
                              '53': 'WA',
                              '54': 'WV',
                              '55': 'WI',
                              '56': 'WY' }

        self.abbr_to_fips = {value: key for key, value in self.fips_to_abbr.items()}

    def all_states(self, abbr=True, sep=' '):
        """
        return a list of US states either in abbreviation or in full names  in the specified format
            users can specify the word separator used in the representation of the name:
                for states whose name contains more than one word such as New York,
                users can specify these words to be connected either by a blank space (the default setting),
                or other separators, such as dash(New-York), plus(New+York) or no space at all (NewYork)

        :param abbr: Boolean
        :param sep: str
        :return: a list of str
        """
        if abbr:
            return list(self.abbr_to_name.keys())
        elif sep == ' ':
            return list(self.abbr_to_name.values())
        else:
            return [name.replace(' ', sep) for name in self.abbr_to_name.values()]

    def abbr2name(self, state_abbr, sep=' '):
        """
        get state name from abbreviation
        can specify the word separator used in the representation of the name:
            for states whose name contains more than one word such as New York,
            users can specify these words to be connected either by a blank space (the default setting),
                or other separators, such as dash(New-York), plus(New+York) or no space at all (NewYork)
        :param state_abbr:str
        :param sep: str
        :return: state_name: str
        """
        state_abbr = state_abbr.strip()
        assert len(state_abbr) == 2 and state_abbr in self.abbr_to_name
        if sep == ' ':
            return self.abbr_to_name[state_abbr]
        else:
            state_name = self.abbr_to_name[state_abbr]
            state_name = state_name.replace(' ', sep)
            return state_name


if __name__ == '__main__':
    uss = USStates()
    print(uss.abbr2name('DC', sep=''))
