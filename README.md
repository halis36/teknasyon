1- http://localhost:8080/ bir HTTP Post isteği atıyoruz (veriler json formatında, postman collecitonu dosyalarda mevcut)
2-gelen verileri producer okuyup kafkaya gönderir
3-kafka verileri işler ve consumer'a gönderir
4-consumer gelen events'taki dataları veritabanına kaydeder.
  -bunun için önce 
    . ana dizinde docker-compose up -d  komnutu ile docker ayağa kaldırılır
    . producer dizininde go run main.go ile producer container'ı çalıştırılır
    . consumer dizininde go run consumer.go consumer container'ı çalıştırılır (consumer anlayamadığım nedenlerle çalışmadı, dolayısı ile veriler db'ye kaydolmadı)
