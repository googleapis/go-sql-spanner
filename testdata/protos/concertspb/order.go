package concertspb

import (
	"database/sql/driver"
	"fmt"

	"github.com/golang/protobuf/proto"
)

func CreateTicketOrder(orderNumber string, date int64, shippingAddress *Address, items []*Item) *TicketOrder {
	return &TicketOrder{
		OrderNumber:     &orderNumber,
		Date:            &date,
		ShippingAddress: shippingAddress,
		LineItem:        items,
	}
}

func CreateAddress(street, city, state, country string) *Address {
	return &Address{
		Street:  &street,
		City:    &city,
		State:   &state,
		Country: &country,
	}
}

func CreateItem(productName string, quantity int32) *Item {
	return &Item{
		ProductName: &productName,
		Quantity:    &quantity,
	}
}

func (x *TicketOrder) Value() (driver.Value, error) {
	return proto.Marshal(x)
}

func (x *TicketOrder) Scan(dest interface{}) error {
	b, ok := dest.([]byte)
	if !ok {
		return fmt.Errorf("invalid type for protobuf value: %v", dest)
	}
	return proto.Unmarshal(b, x)
}
