from requests_html import HTMLSession
import psycopg2
import time, random
# get HTML 

class Journal:
    def __init__(self):
        self.api_key = "7d2640ef332042144a2d05dabf9baa65"
        self.start_url = "https://lyxk.cbpt.cnki.net/WKG/WebPublication/wkTextContent.aspx?colType=4&tp=gklb"
        self.session = HTMLSession()
        self.conn = psycopg2.connect(host="localhost", dbname="geogjournals", user="postgres", 
                        password="Youzidoushi993", port=5432)
        self.cur = self.conn.cursor()

    def get_via_scraperapi (self, target_url):
        proxy_url = f"http://api.scraperapi.com/?api_key={self.api_key}&url={target_url}"
        return self.session.get(proxy_url,timeout=50)

    def get_html (self):
        #log 已爬数据
        done_file = "done.txt" 
        try:
            with open (done_file, 'r') as f:
                done_urls = set(line.strip() for line in f.readlines())
        except FileNotFoundError:
            done_urls = set ()

        #提取主页面
        try:
            response = self.get_via_scraperapi(self.start_url)
            if not response or response.status_code != 200:
                print(f"[ERROR] 请求起始页面失败，状态码: {getattr(response, 'status_code', 'N/A')}")
                return
        except Exception as e:
            print(f"[ERROR] 请求起始页面出错: {e}")
            return
        time.sleep(random.uniform(1, 2))
        
        #遍历年份
        for url in response.html.xpath('//div[@class="pastlistdate"]/ul[@class="year"]//a/@href'):
            url_year = "https://lyxk.cbpt.cnki.net" + url
            res = self.get_via_scraperapi(url_year)
            time.sleep(random.uniform(1, 2)) #进入一个年网址

            #遍历期数
            for urll in res.html.xpath('//div[@class = "pastlistdate"]/ul[@class="date"]//a/@href'):
                url_date = "https://lyxk.cbpt.cnki.net"+urll 

                if url_date in done_urls:
                    print(f"[SKIP] 已完成: {url_date}")
                    continue

                ress = self.get_via_scraperapi(url_date) #进入一个期网址
                time.sleep(random.uniform(1, 2))

                # 构建数据
                for li in ress.html.xpath('//div[@class ="zxlist"]/ul/li'):
                    title = li.xpath('.//h3/a/text()')
                    author = li.xpath('.//samp/text()')
                    time_raw = li.xpath('.//span/text()')
                    time_clean = [t.strip() for t in time_raw if "期" in t]
                    
                    title_text = title[0].strip() if title else ''
                    author_text = author[0].strip() if author else ''
                    time_text = time_clean[0].strip() if time_clean else ''

                    #存入数据库
                    try:
                        sql = 'INSERT INTO tourism (title, author, time) VALUES (%s, %s, %s)'
                        self.cur.execute(sql, (title_text, author_text, time_text)) 
                    except Exception as e:
                        print(f'[ERROR] SQL failed: {title_text}, {author_text}, {time_text}')
                        print(e)
                        self.conn.rollback()
                    else:
                        self.conn.commit()

                # 成功后将 url_date 记录到 done.txt
                with open(done_file, "a") as f:
                    f.write(url_date + "\n")
                print(f"[DONE] 完成: {url_date}")


    def run(self):
        self.get_html()
        self.cur.close()
        self.conn.close()


if __name__ == '__main__':
    download = Journal()
    download.run()