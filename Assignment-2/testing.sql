with recursive temp(userid1, userid2) as 
(
        select *
        from friends_small
    union
        select temp.userid2 as userid1, friends_small.userid1 as userid2
        from temp, friends_small
        where friends_small.userid1 = temp.userid2
 )
select u1.name as name1, t.userid1 as userid1, u2.name as name2, t.userid2 as userid2 
from temp t, users u1, users u2
where t.userid1 = u1.userid and t.userid2 = u2.userid
order by name1, name2;