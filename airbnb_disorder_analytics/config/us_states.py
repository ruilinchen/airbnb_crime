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
