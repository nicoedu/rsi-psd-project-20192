import { Component } from '@angular/core';
import { Map, latLng, tileLayer, Layer, marker } from 'leaflet';
import { HTTP } from '@ionic-native/http/ngx';


@Component({
  selector: 'app-home',
  templateUrl: 'home.page.html',
  styleUrls: ['home.page.scss'],
})
export class HomePage {
  map: Map;
  lat = -8.01746;
  long = -34.9426384;
  constructor(
    private http: HTTP,
    ) {}

  ionViewDidEnter() { this.leafletMap(); }


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

  sentKafkaLatLng(){
    console.log('teste');
  }


}
