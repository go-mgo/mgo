package mgo

type ChangeStream struct {
	iter           *Iter
	options        ChangeStreamOptions
	pipeline       interface{}
	readPreference *ReadPreference
}

type ChangeStreamOptions struct {

	// FullDocument controls the amount of data that the server will return when
	// returning a changes document.
	FullDocument string

	// ResumeAfter specifies the logical starting point for the new change stream.
	ResumeAfter *bson.Raw

	// MaxAwaitTimeMS specifies the maximum amount of time for the server to wait
	// on new documents to satisfy a change stream query.
	MaxAwaitTimeMS int64

	// BatchSize specifies the number of documents to return per batch.
	BatchSize int32

	// Collation specifies the way the server should collate returned data.
	Collation *Collation
}

func constructChangeStreamPipeline(pipeline interface{},
	options ChangeStreamOptions) interface{} {
	pipelinev := reflect.ValueOf(pipeline)

	// ensure that the pipeline passed in is a slice.
	if pipelinev.Kind() != reflect.Slice {
		panic("pipeline argument must be a slice")
	}

	// construct the options to be used by the change notification
	// pipeline stage.
	changeNotificationStageOptions := bson.M{}

	if options.FullDocument != "" {
		changeNotificationStageOptions["fullDocument"] = options.FullDocument
	}
	if options.ResumeAfter != nil {
		changeNotificationStageOptions["resumeAfter"] = options.ResumeAfter
	}
	changeNotificationStage := bson.M{"$changeNotification": changeNotificationStageOptions}

	pipeOfInterfaces := make([]interface{}, pipelinev.Len()+1)

	// insert the change notification pipeline stage at the beginning of the
	// aggregation.
	pipeOfInterfaces[0] = changeNotificationStage

	// convert the passed in slice to a slice of interfaces.
	for i := 0; i < pipelinev.Len(); i++ {
		pipeOfInterfaces[1+i] = pipelinev.Index(i).Addr().Interface()
	}
	var pipelineAsInterface interface{} = pipeOfInterfaces
	return pipelineAsInterface
}
