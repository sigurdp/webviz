export interface Station {
    name: string;
    eoi: string;
}

// https://api.met.no/weatherapi/airqualityforecast/0.1/documentation#%2Fstations

export async function fetchStations(): Promise<Station[]> {
    const res = await fetch("https://api.met.no/weatherapi/airqualityforecast/0.1/stations");
    const json = await res.json();
    const retArr: Station[] = [];
    for (const [key, val] of Object.entries<any>(json)) {
        retArr.push({ name: val.name, eoi: val.eoi });
    }

    return retArr;
}
