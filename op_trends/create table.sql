use ma;
drop table if exists op_trends_tab;
create table op_trends_tab (
				pop_type varchar(max)
				, asked_date date
				, asked_topic varchar(max)
				, value int
				, topic_title varchar(max)
				, topic_type varchar(max)
				);

select * from op_trends_tab;
--update op_trends_tab
--set asked_date = '2021-04-05'