select A1, sum(B)
from r
where A2 < 10 and A2 > 5
group by A1;
