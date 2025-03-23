This code utilizes the New York Times API to get best seller listings by date.Passing in a date will return the full list including all subcategories (nonfiction, childrens, different formats, etc.)
NYT very kindly maintains many APIs and their functionality is straightforward. Data returned is slightly wider than what I wanted to preserve so parts of my code only preserve a subset.

A part of the project also writes parts of the results to a postgres database. The insert statements in the write to postgres package utilize a left join to insert data more elegantly. Additionally, I include a master book list, isbn table and list of lists.

Further work and considerations:
-This code is part of a larger project to combine book reviews and ratings across many sources
-I didn't include the database code yet
-I plan to build off this initial list of books to source other reviews
