sum(case when candidate in ('Orion Webster', 'Mary Silva', 'Derek Chartrand', 'Matt Heines', 'Jeb Brewer') then votes else null end) as cd1_gop_votes,
sum(case when candidate in ('Daniel Miller', 'Cody Hart', 'Leif Johnson') then votes else null end) as cd2_gop_votes,
sum(case when candidate in ('Leslie Lewallen', 'Joe Kent') then votes else null end) as cd3_gop_votes,
sum(case when candidate in ('Jerrod Sessler', 'Dan Newhouse', 'Tiffany Smiley') then votes else null end) as cd4_gop_votes,
sum(case when candidate in ('Rick Valentine Flynn', 'Rene Holaday', 'Jacquelin Maycumber', 'Jonathan D Bingle', 'Michael Baumgartner', 'Brian Dansel') then votes else null end) as cd5_gop_votes,
sum(case when candidate in ('Janis Clark', 'Drew C MacEwen') then votes else null end) as cd6_gop_votes,
sum(case when candidate in ('Cliff Moon') then votes else null end) as cd7_gop_votes,
sum(case when candidate in ('Carmen Goers') then votes else null end) as cd8_gop_votes,
sum(case when candidate in ('Paul Martin', 'Mark Greene') then votes else null end) as cd9_gop_votes,
sum(case when candidate in ('Nirav Sheth', 'Don Hewett') then votes else null end) as cd10_gop_votes,

-- Democratic candidates
sum(case when candidate in ('Suzan DelBene') then votes else null end) as cd1_dem_votes,
sum(case when candidate in ('Edwin Stickle', 'Rick Larsen', 'Devin Hermanson', 'Josh Binda') then votes else null end) as cd2_dem_votes,
sum(case when candidate in ('Marie Gluesenkamp Perez') then votes else null end) as cd3_dem_votes,
sum(case when candidate in ('Mary Baechler', '"Birdie" Jane Muchlinski') then votes else null end) as cd4_dem_votes,
sum(case when candidate in ('Carmela Conroy', 'Ann Marie Danimus', 'Matthew Welde', 'Bernadine Bank', 'Bobbi Bennett-Wolcott') then votes else null end) as cd5_dem_votes,
sum(case when candidate in ('Emily Randall', 'Hilary Franz') then votes else null end) as cd6_dem_votes,
sum(case when candidate in ('Pramila Jayapal', 'Liz Hallock') then votes else null end) as cd7_dem_votes,
sum(case when candidate in ('Kim Schrier', 'Keith Arnold', 'Imraan Siddiqi') then votes else null end) as cd8_dem_votes,
sum(case when candidate in ('Melissa Chaudhry', 'Adam Smith') then votes else null end) as cd9_dem_votes,
sum(case when candidate in ('Marilyn Strickland', 'Eric Mahaffy', 'Desiree C. Toliver') then votes else null end) as cd10_dem_votes

-- Sum for all Democratic candidates across all congressional districts
sum(case when candidate in ('Suzan DelBene', 'Edwin Stickle', 'Rick Larsen', 'Devin Hermanson', 'Josh Binda', 
                            'Marie Gluesenkamp Perez', 'Mary Baechler', '"Birdie" Jane Muchlinski', 
                            'Carmela Conroy', 'Ann Marie Danimus', 'Matthew Welde', 'Bernadine Bank', 
                            'Bobbi Bennett-Wolcott', 'Emily Randall', 'Hilary Franz', 
                            'Pramila Jayapal', 'Liz Hallock', 'Kim Schrier', 'Keith Arnold', 
                            'Imraan Siddiqi', 'Melissa Chaudhry', 'Adam Smith', 
                            'Marilyn Strickland', 'Eric Mahaffy', 'Desiree C. Toliver') 
            then votes else null end) as all_cd_dem_votes,

-- Sum for all GOP/Republican/MAGA candidates across all congressional districts
sum(case when candidate in ('Orion Webster', 'Mary Silva', 'Derek Chartrand', 'Matt Heines', 'Jeb Brewer', 
                            'Daniel Miller', 'Cody Hart', 'Leif Johnson', 'Leslie Lewallen', 'Joe Kent', 
                            'Jerrod Sessler', 'Dan Newhouse', 'Tiffany Smiley', 'Rick Valentine Flynn', 
                            'Rene\' Holaday', 'Jacquelin Maycumber', 'Jonathan D Bingle', 
                            'Michael Baumgartner', 'Brian Dansel', 'Janis Clark', 'Drew C MacEwen', 
                            'Cliff Moon', 'Carmen Goers', 'Paul Martin', 'Mark Greene', 
                            'Nirav Sheth', 'Don Hewett') 
            then votes else null end) as all_cd_gop_votes
