import React from "react";


type TopicMap_Workbench = {
  FieldName: string;
  CaseId: string;
}


type TopicMap_SharedData = {
    InfoMessage: string;
    Depth: number;
    Position: { x: number, y: number}
}

type TopicMapAll = TopicMap_Workbench & TopicMap_SharedData;


//export type Topic = keyof TopicMap;


export class WorkbenchServices {
    private _subscribersMap: { [key: string]: Set<any> } = {}

    subscribe<T extends keyof TopicMapAll>(topic: T, cb: (value: TopicMapAll[T]) => void) {
        const subscribersSet = this._subscribersMap[topic] || new Set();
        subscribersSet.add(cb);
        this._subscribersMap[topic.toString()] = subscribersSet
        return () => {
            subscribersSet.delete(cb);
        }
    }

    publishSharedData<T extends keyof TopicMap_SharedData>(topic: T, value: TopicMap_SharedData[T]) {
        console.log(`PUB SHARED ${topic}=${value}`);
        const subscribersSet = this._subscribersMap[topic] || new Set();
        for (let cb of subscribersSet) {
            cb(value);
        }
    }

    internal_publishWorkbenchData<T extends keyof TopicMap_Workbench>(topic: T, value: TopicMap_Workbench[T]) {
      console.log(`PUB WORKBENCH ${topic}=${value}`);
      const subscribersSet = this._subscribersMap[topic] || new Set();
      for (let cb of subscribersSet) {
          cb(value);
      }
  }
}


export function useWorkbenchSubscribedValue<T extends keyof TopicMapAll>(topic: T, workbenchServices: WorkbenchServices): TopicMapAll[T] | null {
  const [latestValue, setLatestValue] = React.useState<TopicMapAll[T]|null>(null);

  React.useEffect(function subscribeToWorkbench() {
      function handleNewMessageFromWorkbench(newValue: TopicMapAll[T]) {
        setLatestValue(newValue);
      }
  
      const unsubscribeFunc = workbenchServices.subscribe(topic, handleNewMessageFromWorkbench)
      return unsubscribeFunc;
  }, [workbenchServices]);

  return latestValue;
}




// To think a bit more ablout...
// export class WorkbenchServicesInternal extends WorkbenchServices {
//   publishWorkbenchData<T extends keyof TopicMap_Workbench>(topic: T, value: TopicMap_Workbench[T]) {
//       console.log(`PUB ${topic}=${value}`);
//       const subscribersSet = this._workbenchDataSubscribersMap[topic] || new Set();
//       for (let cb of subscribersSet) {
//           cb(value);
//       }
//   }
// }


