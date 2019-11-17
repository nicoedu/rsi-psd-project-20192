import { Component } from '@angular/core';
import { Map, latLng, tileLayer, Layer, marker } from 'leaflet';
import { HttpClient, HttpHeaders } from '@angular/common/http';


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
    public http: HttpClient
  ) { }

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

  sentKafkaLatLng() {
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
        console.log(data['_body']);
      }, error => {
        console.log(error);
      });
  }


}
