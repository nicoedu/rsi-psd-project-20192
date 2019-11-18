import { Component } from '@angular/core';
import { Map, latLng, tileLayer, Layer, marker } from 'leaflet';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { AlertController } from '@ionic/angular';


@Component({
  selector: 'app-home',
  templateUrl: 'home.page.html',
  styleUrls: ['home.page.scss'],
})
export class HomePage {
  map: Map;
  lat = -8.01746;
  long = -34.9426384;
  enabledButton = true;
  constructor(
    public http: HttpClient,
    public alertController: AlertController
  ) { }

  ionViewDidEnter() { this.leafletMap(); }

  async presentAlert(titulo, subtitulo, message) {
    const alert = await this.alertController.create({
      header: titulo,
      subHeader: subtitulo,
      message,
      buttons: ['OK']
    });

    await alert.present();
  }


  leafletMap() {
    this.map = new Map('map').setView([this.lat, this.long], 13);

    tileLayer('http://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png', {
      attribution: 'edupala.com'
    }).addTo(this.map);

    const markPoint = marker([this.lat, this.long]);
    markPoint.bindPopup('<p>Localização atual</p>');
    this.map.addLayer(markPoint);

    this.map.on('click', (e) => {
      this.lat = e.latlng.lat;
      this.long = e.latlng.lng;
      markPoint.setLatLng(e.latlng);
    });
  }

  ionViewWillLeave() {
    this.map.remove();
  }
  calcAlert(heatIndex) {
    if (heatIndex <= 27) {
      this.presentAlert('Nível de alerta:', 'Normal', `O índice de calor é de ${heatIndex}º C`);
    } else if ( heatIndex > 27 && heatIndex <= 32 ) {
      this.presentAlert('Nível de alerta:', 'Cautela', `O índice de calor é de ${heatIndex}º C`);
    } else if ( heatIndex > 32 && heatIndex <= 41 ) {
      this.presentAlert('Nível de alerta:', 'Cautela extrema', `O índice de calor é de ${heatIndex}º C`);
    } else if ( heatIndex > 41 && heatIndex <= 54 ) {
      this.presentAlert('Nível de alerta:', 'Perigo', `O índice de calor é de ${heatIndex}º C`);
    } else {
      this.presentAlert('Nível de alerta:', 'Perigo extremo', `O índice de calor é de ${heatIndex}º C`);
    }

  }
  sentKafkaLatLng() {
    this.enabledButton = false;
    const httpOptions = {
      headers: new HttpHeaders({
        'Content-Type': 'application/json'
      })
    };

    let postData = {
      "latitude": this.lat,
      "longitude": this.long
    }


    this.http.post("http://localhost:3000/", postData, httpOptions)
      .subscribe(data => {
        this.enabledButton = true;
        this.calcAlert(data);
      }, error => {
        this.enabledButton = true;
        this.presentAlert('Erro', 'Tente novamente', 'Não foi possível calcular o índice de calor do local.')

        console.log(error);
      });
  }


}

// if heatIndex <= 27:
// print("Índice de Calor: %.2fº C  Nível de Alerta:%s \n" % (heatIndex, "Normal"))
// elif heatIndex <= 32 and heatIndex > 27:
// print("Índice de Calor: %.2fº C  Nível de Alerta:%s \n" % (heatIndex, "Cautela"))
// elif heatIndex <= 41 and heatIndex > 32:
// print("Índice de Calor: %.2fº C  Nível de Alerta:%s \n" % (heatIndex, "Cautela extrema"))
// elif heatIndex <= 54 and heatIndex > 41:
// print("Índice de Calor: %.2fº C  Nível de Alerta:%s \n" % (heatIndex, "Perigo"))
// else:
// print("Índice de Calor: %.2fº C  Nível de Alerta:%s \n" % (heatIndex, "Perigo extremo"))