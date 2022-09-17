queries = ["" for i in range(0, 25)]

### 0. List all the users who have at least 1000 UpVotes.
### Output columns and order: Id, Reputation, CreationDate, DisplayName
### Order by Id ascending
queries[0] = """
select Id, Reputation, CreationDate,  DisplayName
from users
where UpVotes >= 1000
order by Id asc;
"""

### 1. List the posts (Id, Title, Tags) for all posts that are tagged 'postgresql-9.4'
### Hint: use ``like'' -- note that tags are enclosed in '<>' in the Tags field.
### Output column order: Id, Title, Tags
### Order by Id ascending
queries[1] = """
select Id, Title, Tags
from posts
where tags like '%<postgresql-9.4>%'
order by Id asc;
"""


### 2. Write a query to output the number of years users have been on the
### platform (assuming they started on 'CreationDate') as of September 1, 2022
### Use 'age' function that operates on dates (https://www.postgresql.org/docs/12/functions-datetime.html)
### Restrict output to Users with DisplayName = 'Jason'
### Output columns: Id, DisplayName, Num_years
### Order output by Num_years increasing, and then by Id ascending
queries[2] = """
select Id, DisplayName, extract(year from age('2022-09-01', CreationDate)) as Num_years 
from users where DisplayName='Jason'
order by num_years asc, Id asc ;
"""

### 3. Select all the "distinct" years that users with names starting with 'M'
### joined the platform (i.e., created their accounts).
### Output column: Year
### Order output by Year ascending
queries[3] = """
select distinct extract(year from CreationDate) as Year
from users where displayname like 'M%'
order by Year asc;
"""

### 4. Write a query to find users who have, on average, given at least 1 UpVote per
### day they have been on the platform as of September 1, 2022
### Hint: Use subtraction on "date" to get number of days between two dates.
### Count only full days (i.e., someone who joined 1.5 days ago only needs to
### have provided 1 UpVote to make it into the result)
### Output columns: Id, DisplayName, CreationDate, UpVotes
### Order by Id ascending
queries[4] = """
select Id, DisplayName, CreationDate, UpVotes
from users
where Upvotes > 0 and date_part( 'day', '2022-09-01'::timestamp - CreationDate) > 0 and UpVotes / date_part( 'day', '2022-09-01'::timestamp - CreationDate) >= 1
order by Id asc;
"""

### 5. Write a single query to report all Badges for the users with reputation between 10000 and 11000, by joining Users and Badges.
### Output Column: Id (from Users), DisplayName, Name (from Badges), Reputation
### Order by: Id increasing
queries[5] = """
select users.id as id, displayname, badges.name as name, reputation
from users join badges on (
        reputation >= 10000 and 
        reputation <= 11000 and 
        users.id = badges.userid)
order by id asc;
"""

'''
select users.id as id, displayname, badges.name as name, reputation
from users join badges on (
        reputation >= 10000 and 
        reputation <= 11000 and 
        users.id = badges.userid)
order by id asc;
'''

### 6. Write a query to find all Posts who satisfy one of the following conditions:
###        - the post title contains 'postgres' and the number of views is at least 50000
###        - the post title contains 'mongodb' and the number of views is at least 25000
### The match should be case insensitive
### Output columns: Id, Title, ViewCount
### Order by: Id ascending
queries[6] = """
select Id, Title, ViewCount
from posts
where 
        (Title ilike '%postgres%' and ViewCount >= 50000) or 
        (Title ilike '%mongodb%' and ViewCount >= 25000)
order by Id asc;
"""



### 7. Count the number of the Comments made by the user with DisplayName 'JHFB'.
### Output columns: Num_Comments
queries[7] = """
select count(*) as Num_Comments
from comments
where userid in
        (select id from users
        where displayname = 'JHFB');
"""

### 8. Find the Users who have received badges with names: "Guru" and "Curious". 
### Only report a user once even if they have received multiple badges with the above names.
### Hint: Use Intersect.
### Output columns: UserId
### Order by: UserId ascending
queries[8] = """
select userid from badges where name='Guru'
intersect
select userid from badges where name='Curious'
order by userid asc;
"""

### 9. "Tags" field in Posts lists out the tags associated with the post in the format "<tag1><tag2>..<tagn>".
### Find the Posts with at least 6 tags, with one of the tags being postgresql (exact match).
### Hint: use "string_to_array" and "cardinality" functions.
### Output columns: Id, Title, Tags
### Order by: Id ascending
queries[9] = """
select Id, Title, Tags
from posts
group by id
        having Tags like '%<postgresql>%' and 
        cardinality(string_to_array(Tags, '><')) >= 6
order by id asc;
"""


### 10. SQL "with" clause can be used to simplify queries. It essentially allows
### specifying temporary tables to be used during the rest of the query. See Section
### 3.8.6 (6th Edition) for some examples.
###
### Write a query to find the name(s) of the user(s) with the largest number of Comments. 
### We have provided a part of the query to build a temporary table.
###
### Output columns: Id, DisplayName, Num_Comments
### Order by Id ascending (there may be more than one answer)
queries[10] = """
with temp as (
        select userid, count(*) as num_comments
        from comments
        group by userid
)
select userid as id, displayname, num_comments
from temp, users
where num_comments = (
        select max(num_comments)
        from temp
) and temp.userid = users.id
order by temp.userid asc;
"""
# does not work
'''
with temp as (
        select Users.Id, DisplayName, count(*) as num_Comments 
        from users, comments 
        where users.id = comments.userid 
        group by users.id, users.displayname)
select users.id, users.displayname, temp.num_comments
from users, temp
where temp.num_comments = (
        select max(temp.num_comments)
        from temp
)
order by users.id asc
;
'''

### 11. List the users who posted no comments and with at least 500 views. 
### Hint: Use "not in".
### Output Columns: Id, DisplayName
### Order by Id ascending
queries[11] = """
select id, displayname
from users
where 
        views >= 500 and
        id not in (select userid from comments)
order by id asc;
"""


### 12. Write a query to output a list of posts with comments, such that PostType = 'Moderator nomination' 
### and the comment has score of at least 10. So there may be multiple rows with the same post
### in the output.
### Output: Id (Posts), Title, Text (Comments)
### Order by: Id ascending
queries[12] = """
select posts.id as Id, posts.title as Title, comments.text as Text
from posts, comments, posttypes
where
        posts.posttypeid = posttypes.posttypeid
        and comments.postid = posts.id
        and posttypes.description = 'Moderator nomination'
        and comments.score >= 10
order by posts.id asc;
"""
'''
posttypes (posttypeid, description)
posts (id, posttypeid, title)
comments (id, text, postid, score)
'''

### 13. Generate a list - (Badge_Name, Num_Users) - containing the different
### badges, and the number of users who received those badges.
### Note: A user may receive the same badge multiple times -- they should only be counted once for that badge.
### Output columns: Badge_name, Num_users
### Order by Badge_name asc
### Use LIMIT to limit the output to first 20 rows.
queries[13] = """
select name as badge_name, count(userid) as num_users
from (select distinct name , userid from badges) as foo
group by badge_name
order by badge_name asc
limit 20;
"""

#TODO gives same names but diff num, dk why, come back for revision
"""
select distinct name as Badge_Name, count(userid) as Num_Users
from badges
group by badge_name
order by Badge_Name asc
limit 20;
"""


### 14. For each post, count the number of comments for that post.
###
### One way to do this is "Scalar" subqueries in the select clause.
### select Id,                             
### (select count(*) from comments where comments.postid = posts.id) as Num_comments
### from posts order by posts.id;
### 
### However, this takes too long, even on the relatively small database we
### have.
###
### Instead, use "left outer join" to do this task.
###
### Output Columns: Id, Num_Comments
### Order by: Id ascending
queries[14] = """
select posts.id as Id, count(comments.id) as num_comments
from posts left outer join comments
        on (posts.id = comments.postid)
group by posts.id
order by Id asc;
"""


### 15. Generate a list - (Reputation, Num_Users) - containing the number
### of users with reputation between 1 and 100 (inclusive). If a particular reputation
### score does not have any users (e.g., 2), then that reputation should appear with a
### 0 count.
###
### HINT: Use "generate_series()" to create an inline table -- try 
### "select * from generate_series(1, 10) as g(n);" to see how it works.
### This is what's called a "set returning function", and the result can be used as a relation.
### See: https://www.postgresql.org/docs/12/functions-srf.html
###
### Output columns: Reputation, Num_users
### Order by Reputation ascending
queries[15] = """
select r as Reputation, count(id) as Num_Users
from generate_series(1,100) as g(r) 
        left outer join users
on (r = users.reputation)
group by r
order by r asc;
"""


### 16. Generalizing #14 above, associate posts with both the number of
### comments and the number of votes
### 
#14:
"""
select posts.id as Id, count(comments.id) as num_comments
from posts left outer join comments
        on (posts.id = comments.postid)
group by posts.id
order by Id asc;
"""

### As above, using scalar subqueries won't scale to the number of tuples.
### Instead use WITH and Left Outer Joins.
###
### Output Columns: Id, Num_Comments, Num_Votes
### Order by: Id ascending
queries[16] = """
with temp(Id, num_comments) as (
        select posts.id as Id, count(comments.id) as num_comments
        from posts left outer join comments on (posts.id = comments.postid)
        group by posts.id
        order by id asc
)
select temp.id as Id, temp.num_comments as num_comments, count(votes.id) as num_votes
from temp left outer join votes on (temp.id = votes.postid)
group by temp.id, temp.num_comments
order by id asc;
"""


### 17. Write a query to find the posts with at least 7 children (i.e., at
### least 7 other posts with that post as the parent
###
### Output columns: Id, Title
### Order by: Id ascending
queries[17] = """
with p as (
        select parentid, count(parentid)
        from posts
        group by parentid
        having count(parentid) >= 7
)
select posts.id as id, posts.title as title
from posts, p
where posts.id = p.parentid
group by posts.id
order by posts.id asc;
"""

### 18. Find posts such that, between the post and its children (i.e., answers
### to that post), there are a total of 100 or more votes
###
### HINT: Use "union all" to create an appropriate temp table using WITH
###
### Output columns: Id, Title
### Order by: Id ascending

# 
queries[18] = """
with
        v as (
                select postid, count(postid) as vote_count
                from votes
                group by postid
        ),
        u as (
                select distinct parentid
                from posts
                where parentid is not null
        ),
        w as (
                select u.parentid as id, sum(v.vote_count) as total
                from u, v
                where v.postid = u.parentid
                group by u.parentid
        )
select w.id, posts.title
from w, posts, v, u
where v.vote_count + w.total >= 100 and posts.id = w.id and v.postid = w.id
group by w.id, posts.title
order by w.id asc;
"""


### 19. Write a query to find posts where the post and the accepted answer
### are both owned by the same user (i.e., have the same "OwnerUserId") and the
### user has not made any other post (outside of those two).
###
### Hint: Use "not exists" for the last one.
###
### Output columns: Id, Title
### Order by: Id Ascending

# p = posts copy, o = posts with owner2
queries[19] = """
with p as (
        select * from posts
),
o as (
        select owneruserid, count(owneruserid)
        from posts
        group by owneruserid
        having count(owneruserid) = 2
        order by owneruserid asc
)
select p.id, p.title
from posts, p, o
where (
        posts.id = p.acceptedanswerid and
        posts.owneruserid = p.owneruserid and
        o.owneruserid = p.owneruserid
)        
group by p.id, p.title
order by p.id asc;
"""


### 20. Write a query to generate a table: 
### (VoteTypeDescription, Day_of_Week, Num_Votes)
### where we count the number of votes corresponding to each combination
### of vote type and Day_of_Week (obtained by extract "dow" on CreationDate).
### So Day_of_Week will take values from 0 to 6 (Sunday to Saturday resp.)
###
### Don't worry if a particular combination of Description and Day of Week has 
### no votes -- there should be no row in the output for that combination.
###
### Output column order: VoteTypeDescription, Day_of_Week, Num_Votes
### Order by VoteTypeDescription asc, Day_of_Week asc
queries[20] = """
select 
        votetypes.description as votetypedescription, 
        extract(dow from votes.creationdate), 
        count(votes.id) as num_votes
from votes
        join votetypes
        on (votes.votetypeid = votetypes.votetypeid)
group by 
        votetypes.description, 
        extract(dow from votes.creationdate)
order by 
        votetypes.description asc, 
        extract(dow from votes.creationdate) asc;
"""

