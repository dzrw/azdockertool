/*
This package manipulates the contents of a remote filesystem containing images
built from Docker 1.10+ (API Version 1.23 onward).

Container Structure
===================
layers/{LAYER_ID}/{VERSION, json, layer.tar}
images/{IMAGE_ID}/{manifest.json, repositories, json}
refs/{REPOSITORY}/{TAG}
*/
package azdockertool
